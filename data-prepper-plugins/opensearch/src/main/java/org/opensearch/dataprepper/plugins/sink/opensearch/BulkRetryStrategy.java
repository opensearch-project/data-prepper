/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.Counter;
import org.opensearch.OpenSearchException;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opensearch.dataprepper.plugins.sink.opensearch.ErrorCauseStringCreator.toSingleLineDisplayString;

public final class BulkRetryStrategy {
    public static final String DOCUMENTS_SUCCESS = "documentsSuccess";
    public static final String DOCUMENTS_SUCCESS_FIRST_ATTEMPT = "documentsSuccessFirstAttempt";
    public static final String DOCUMENT_ERRORS = "documentErrors";
    public static final String DOCUMENT_NUMBER_OF_RETRIES = "documentNumberOfRetries";
    public static final String DOCUMENT_NUMBER_OF_FAILURES = "documentNumberOfFailures";
    public static final String DOCUMENT_BAD_REQUEST_ERRORS = "documentBadRequestErrors";
    public static final String DOCUMENT_NOT_ALLOWED_ERRORS = "documentNotAllowedErrors";
    public static final String DOCUMENT_INVALID_INPUT_ERRORS = "documentInvalidInputErrors";
    public static final String DOCUMENT_NOT_FOUND_ERRORS = "documentNotFoundErrors";
    public static final String DOCUMENT_TIMEOUT_ERRORS = "documentTimeoutErrors";
    public static final String DOCUMENT_SERVER_ERRORS = "documentServerErrors";

    private static final Set<Integer> NON_RETRY_STATUS = new HashSet<>(
            Arrays.asList(
                    RestStatus.BAD_REQUEST.getStatus(),
                    RestStatus.NOT_FOUND.getStatus(),
                    RestStatus.CONFLICT.getStatus()
            ));

    private static final Set<Integer> BAD_REQUEST_ERRORS = new HashSet<>(
            Arrays.asList(
                    RestStatus.BAD_REQUEST.getStatus(),
                    RestStatus.EXPECTATION_FAILED.getStatus(),
                    RestStatus.UNPROCESSABLE_ENTITY.getStatus(),
                    RestStatus.FAILED_DEPENDENCY.getStatus(),
                    RestStatus.NOT_ACCEPTABLE.getStatus()
            ));

    private static final Set<Integer> NOT_ALLOWED_ERRORS = new HashSet<>(
            Arrays.asList(
                    RestStatus.UNAUTHORIZED.getStatus(),
                    RestStatus.FORBIDDEN.getStatus(),
                    RestStatus.PAYMENT_REQUIRED.getStatus(),
                    RestStatus.METHOD_NOT_ALLOWED.getStatus(),
                    RestStatus.PROXY_AUTHENTICATION.getStatus(),
                    RestStatus.LOCKED.getStatus(),
                    RestStatus.TOO_MANY_REQUESTS.getStatus()
            ));

    private static final Set<Integer> INVALID_INPUT_ERRORS = new HashSet<>(
            Arrays.asList(
                    RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus(),
                    RestStatus.REQUEST_URI_TOO_LONG.getStatus(),
                    RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(),
                    RestStatus.LENGTH_REQUIRED.getStatus(),
                    RestStatus.PRECONDITION_FAILED.getStatus(),
                    RestStatus.UNSUPPORTED_MEDIA_TYPE.getStatus(),
                    RestStatus.CONFLICT.getStatus()
            ));

    private static final Set<Integer> NOT_FOUND_ERRORS = new HashSet<>(
            Arrays.asList(
                    RestStatus.NOT_FOUND.getStatus(),
                    RestStatus.GONE.getStatus()
            ));

    private static final Set<Integer> TIMEOUT_ERROR = new HashSet<>(
            Arrays.asList(
                    RestStatus.REQUEST_TIMEOUT.getStatus()
            ));

    private final RequestFunction<AccumulatingBulkRequest<BulkOperation, BulkRequest>, BulkResponse> requestFunction;
    private final BiConsumer<BulkOperation, Throwable> logFailure;
    private final PluginMetrics pluginMetrics;
    private final Supplier<AccumulatingBulkRequest> bulkRequestSupplier;

    private final Counter sentDocumentsCounter;
    private final Counter sentDocumentsOnFirstAttemptCounter;
    private final Counter documentErrorsCounter;
    private final Counter documentNumberOfRetries;
    private final Counter documentNumberOfFailures;
    private final Counter documentBadRequestErrors;
    private final Counter documentNotAllowedErrors;
    private final Counter documentInvalidInputErrors;
    private final Counter documentNotFoundErrors;
    private final Counter documentTimeoutErrors;
    private final Counter documentServerErrors;

    public BulkRetryStrategy(final RequestFunction<AccumulatingBulkRequest<BulkOperation, BulkRequest>, BulkResponse> requestFunction,
                             final BiConsumer<BulkOperation, Throwable> logFailure,
                             final PluginMetrics pluginMetrics,
                             final Supplier<AccumulatingBulkRequest> bulkRequestSupplier) {
        this.requestFunction = requestFunction;
        this.logFailure = logFailure;
        this.pluginMetrics = pluginMetrics;
        this.bulkRequestSupplier = bulkRequestSupplier;

        sentDocumentsCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS);
        sentDocumentsOnFirstAttemptCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS_FIRST_ATTEMPT);
        documentErrorsCounter = pluginMetrics.counter(DOCUMENT_ERRORS);
        documentNumberOfRetries = pluginMetrics.counter(DOCUMENT_NUMBER_OF_RETRIES);
        documentNumberOfFailures = pluginMetrics.counter(DOCUMENT_NUMBER_OF_FAILURES);
        documentBadRequestErrors = pluginMetrics.counter(DOCUMENT_BAD_REQUEST_ERRORS);
        documentNotAllowedErrors = pluginMetrics.counter(DOCUMENT_NOT_ALLOWED_ERRORS);
        documentInvalidInputErrors = pluginMetrics.counter(DOCUMENT_INVALID_INPUT_ERRORS);
        documentNotFoundErrors = pluginMetrics.counter(DOCUMENT_NOT_FOUND_ERRORS);
        documentTimeoutErrors = pluginMetrics.counter(DOCUMENT_TIMEOUT_ERRORS);
        documentServerErrors = pluginMetrics.counter(DOCUMENT_SERVER_ERRORS);
    }

    public void execute(final AccumulatingBulkRequest bulkRequest) throws InterruptedException {
        // Exponential backoff run forever
        // TODO: replace with custom backoff policy setting including maximum interval between retries
        final BackOffUtils backOffUtils = new BackOffUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), Integer.MAX_VALUE).iterator());
        handleRetry(bulkRequest, null, backOffUtils, true);
    }

    public boolean canRetry(final BulkResponse response) {
        for (final BulkResponseItem bulkItemResponse : response.items()) {
            if (bulkItemResponse.error() != null && !NON_RETRY_STATUS.contains(bulkItemResponse.status())) {
                return true;
            }
        }
        return false;
    }

    public boolean canRetry(final Exception e) {
        return (e instanceof IOException ||
                (e instanceof OpenSearchException &&
                        !NON_RETRY_STATUS.contains(((OpenSearchException) e).status().getStatus())));
    }

    private void handleRetry(final AccumulatingBulkRequest request, final BulkResponse response,
                             final BackOffUtils backOffUtils, final boolean firstAttempt) throws InterruptedException {
        final AccumulatingBulkRequest<BulkOperation, BulkRequest> bulkRequestForRetry = createBulkRequestForRetry(request, response);
        if (backOffUtils.hasNext()) {
            // Wait for backOff duration
            backOffUtils.next();
            final BulkResponse bulkResponse;
            try {
                bulkResponse = requestFunction.apply(bulkRequestForRetry);
            } catch (final Exception e) {
                int status = ((OpenSearchException) e).status().getStatus();
                if (NOT_ALLOWED_ERRORS.contains(status)) {
                    documentNotAllowedErrors.increment();
                } else if (INVALID_INPUT_ERRORS.contains(status)) {
                    documentInvalidInputErrors.increment();
                } else if (NOT_FOUND_ERRORS.contains(status)) {
                    documentNotFoundErrors.increment();
                } else if (status == RestStatus.REQUEST_TIMEOUT.getStatus()) {
                    documentTimeoutErrors.increment();
                } else if (status >= RestStatus.INTERNAL_SERVER_ERROR.getStatus() && status <= RestStatus.INSUFFICIENT_STORAGE.getStatus()) {
                    documentServerErrors.increment();
                } else { // Default to Bad Requests
                    documentBadRequestErrors.increment();
                }

                if (canRetry(e)) {
                    handleRetry(bulkRequestForRetry, null, backOffUtils, false);
                    documentNumberOfRetries.increment();
                } else {
                    handleFailures(bulkRequestForRetry, e);
                    documentNumberOfFailures.increment();
                }

                return;
            }
            if (bulkResponse.errors()) {
                if (canRetry(bulkResponse)) {
                    if (firstAttempt) {
                        for (final BulkResponseItem bulkItemResponse : bulkResponse.items()) {
                            if (bulkItemResponse.error() == null) {
                                sentDocumentsOnFirstAttemptCounter.increment();
                            }
                        }
                    }
                    handleRetry(bulkRequestForRetry, bulkResponse, backOffUtils, false);
                } else {
                    handleFailures(bulkRequestForRetry, bulkResponse.items());
                }
            } else {
                final int numberOfDocs = bulkRequestForRetry.getOperationsCount();
                if (firstAttempt) {
                    sentDocumentsOnFirstAttemptCounter.increment(numberOfDocs);
                }
                sentDocumentsCounter.increment(bulkRequestForRetry.getOperationsCount());
            }
        }
    }

    private AccumulatingBulkRequest<BulkOperation, BulkRequest> createBulkRequestForRetry(
            final AccumulatingBulkRequest<BulkOperation, BulkRequest> request, final BulkResponse response) {
        if (response == null) {
            // first attempt or retry due to Exception
            return request;
        } else {
            final AccumulatingBulkRequest requestToReissue = bulkRequestSupplier.get();
            int index = 0;
            for (final BulkResponseItem bulkItemResponse : response.items()) {
                if (bulkItemResponse.error() != null) {
                    if (!NON_RETRY_STATUS.contains(bulkItemResponse.status())) {
                        requestToReissue.addOperation(request.getOperationAt(index));
                    } else {
                        // log non-retryable failed request
                        logFailure.accept(request.getOperationAt(index), new RuntimeException(toSingleLineDisplayString(bulkItemResponse.error())));
                        documentErrorsCounter.increment();
                    }
                } else {
                    sentDocumentsCounter.increment();
                }
                index++;
            }
            return requestToReissue;
        }
    }

    private void handleFailures(final AccumulatingBulkRequest<BulkOperation, BulkRequest> accumulatingBulkRequest, final List<BulkResponseItem> itemResponses) {
        assert accumulatingBulkRequest.getOperationsCount() == itemResponses.size();
        for (int i = 0; i < itemResponses.size(); i++) {
            final BulkResponseItem bulkItemResponse = itemResponses.get(i);
            final BulkOperation bulkOperation = accumulatingBulkRequest.getOperationAt(i);
            if (bulkItemResponse.error() != null) {
                final ErrorCause error = bulkItemResponse.error();
                logFailure.accept(bulkOperation, new RuntimeException(toSingleLineDisplayString(error)));
                documentErrorsCounter.increment();
            } else {
                sentDocumentsCounter.increment();
            }
        }
    }

    private void handleFailures(final AccumulatingBulkRequest<BulkOperation, BulkRequest> accumulatingBulkRequest, final Throwable failure) {
        documentErrorsCounter.increment(accumulatingBulkRequest.getOperationsCount());
        for (final BulkOperation bulkOperation: accumulatingBulkRequest.getOperations()) {
            logFailure.accept(bulkOperation, failure);
        }
    }
}
