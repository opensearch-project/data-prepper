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
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opensearch.dataprepper.plugins.sink.opensearch.ErrorCauseStringCreator.toSingleLineDisplayString;

public final class BulkRetryStrategy {
    public static final String DOCUMENTS_SUCCESS = "documentsSuccess";
    public static final String DOCUMENTS_SUCCESS_FIRST_ATTEMPT = "documentsSuccessFirstAttempt";
    public static final String DOCUMENT_ERRORS = "documentErrors";
    public static final String BULK_REQUEST_FAILED = "bulkRequestFailed";
    public static final String BULK_REQUEST_NUMBER_OF_RETRIES = "bulkRequestNumberOfRetries";
    public static final String BULK_BAD_REQUEST_ERRORS = "bulkBadRequestErrors";
    public static final String BULK_REQUEST_NOT_ALLOWED_ERRORS = "bulkRequestNotAllowedErrors";
    public static final String BULK_REQUEST_INVALID_INPUT_ERRORS = "bulkRequestInvalidInputErrors";
    public static final String BULK_REQUEST_NOT_FOUND_ERRORS = "bulkRequestNotFoundErrors";
    public static final String BULK_REQUEST_TIMEOUT_ERRORS = "bulkRequestTimeoutErrors";
    public static final String BULK_REQUEST_SERVER_ERRORS = "bulkRequestServerErrors";

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
    private final int maxRetries;
    private final Map<AccumulatingBulkRequest<BulkOperation, BulkRequest>, Integer> retryCountMap;

    private final Counter sentDocumentsCounter;
    private final Counter sentDocumentsOnFirstAttemptCounter;
    private final Counter documentErrorsCounter;
    private final Counter bulkRequestFailedCounter;
    private final Counter bulkRequestNumberOfRetries;
    private final Counter bulkRequestBadErrors;
    private final Counter bulkRequestNotAllowedErrors;
    private final Counter bulkRequestInvalidInputErrors;
    private final Counter bulkRequestNotFoundErrors;
    private final Counter bulkRequestTimeoutErrors;
    private final Counter bulkRequestServerErrors;

    public BulkRetryStrategy(final RequestFunction<AccumulatingBulkRequest<BulkOperation, BulkRequest>, BulkResponse> requestFunction,
                             final BiConsumer<BulkOperation, Throwable> logFailure,
                             final PluginMetrics pluginMetrics,
                             final int maxRetries,
                             final Supplier<AccumulatingBulkRequest> bulkRequestSupplier) {
        this.requestFunction = requestFunction;
        this.logFailure = logFailure;
        this.pluginMetrics = pluginMetrics;
        this.bulkRequestSupplier = bulkRequestSupplier;
        this.maxRetries = maxRetries;
        this.retryCountMap = new HashMap<>();

        sentDocumentsCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS);
        sentDocumentsOnFirstAttemptCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS_FIRST_ATTEMPT);
        documentErrorsCounter = pluginMetrics.counter(DOCUMENT_ERRORS);
        bulkRequestFailedCounter = pluginMetrics.counter(BULK_REQUEST_FAILED);
        bulkRequestNumberOfRetries = pluginMetrics.counter(BULK_REQUEST_NUMBER_OF_RETRIES);
        bulkRequestBadErrors = pluginMetrics.counter(BULK_BAD_REQUEST_ERRORS);
        bulkRequestNotAllowedErrors = pluginMetrics.counter(BULK_REQUEST_NOT_ALLOWED_ERRORS);
        bulkRequestInvalidInputErrors = pluginMetrics.counter(BULK_REQUEST_INVALID_INPUT_ERRORS);
        bulkRequestNotFoundErrors = pluginMetrics.counter(BULK_REQUEST_NOT_FOUND_ERRORS);
        bulkRequestTimeoutErrors = pluginMetrics.counter(BULK_REQUEST_TIMEOUT_ERRORS);
        bulkRequestServerErrors = pluginMetrics.counter(BULK_REQUEST_SERVER_ERRORS);
    }

    public void execute(final AccumulatingBulkRequest bulkRequest) throws InterruptedException {
        // Exponential backoff run forever
        // TODO: replace with custom backoff policy setting including maximum interval between retries
        final BackOffUtils backOffUtils = new BackOffUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), Integer.MAX_VALUE).iterator());
        handleRetry(bulkRequest, null, backOffUtils);
    }

    public boolean canRetry(final BulkResponse response) {
        for (final BulkResponseItem bulkItemResponse : response.items()) {
            if (bulkItemResponse.error() != null && !NON_RETRY_STATUS.contains(bulkItemResponse.status())) {
                return true;
            }
        }
        return false;
    }

    public static boolean canRetry(final Exception e) {
        return (e instanceof IOException ||
                (e instanceof OpenSearchException &&
                        !NON_RETRY_STATUS.contains(((OpenSearchException) e).status().getStatus())));
    }

    private void handleRetriesAndFailures(final AccumulatingBulkRequest bulkRequestForRetry,
                                          final int retryCount,
                                          final BackOffUtils backOffUtils,
                                          final BulkResponse bulkResponse,
                                          Exception e) throws InterruptedException {
        boolean doRetry = (Objects.isNull(e)) ? canRetry(bulkResponse) : canRetry(e);
        if (!Objects.isNull(bulkResponse) && doRetry && retryCount == 1) { // first attempt
            for (final BulkResponseItem bulkItemResponse : bulkResponse.items()) {
                if (bulkItemResponse.error() == null) {
                    sentDocumentsOnFirstAttemptCounter.increment();
                }
            }
        }
        if (doRetry && retryCount < maxRetries) {
            handleRetry(bulkRequestForRetry, bulkResponse, backOffUtils);
            bulkRequestNumberOfRetries.increment();
        } else {
            if (doRetry && retryCount >= maxRetries) {
                e = new RuntimeException(String.format("Number of retries reached the limit of max retries(configured value %d)", maxRetries));
            }
            if (Objects.isNull(e)) {
                handleFailures(bulkRequestForRetry, bulkResponse.items());
            } else {
                handleFailures(bulkRequestForRetry, e);
            }
            bulkRequestFailedCounter.increment();
        }
    }

    private void handleRetry(final AccumulatingBulkRequest request, final BulkResponse response,
                             final BackOffUtils backOffUtils) throws InterruptedException {
        final AccumulatingBulkRequest<BulkOperation, BulkRequest> bulkRequestForRetry = createBulkRequestForRetry(request, response);
        int retryCount = retryCountMap.get(bulkRequestForRetry);
        if (backOffUtils.hasNext()) {
            // Wait for backOff duration
            backOffUtils.next();
            final BulkResponse bulkResponse;
            try {
                bulkResponse = requestFunction.apply(bulkRequestForRetry);
            } catch (Exception e) {
                if (e instanceof OpenSearchException) {
                    int status = ((OpenSearchException) e).status().getStatus();
                    if (NOT_ALLOWED_ERRORS.contains(status)) {
                        bulkRequestNotAllowedErrors.increment();
                    } else if (INVALID_INPUT_ERRORS.contains(status)) {
                        bulkRequestInvalidInputErrors.increment();
                    } else if (NOT_FOUND_ERRORS.contains(status)) {
                        bulkRequestNotFoundErrors.increment();
                    } else if (status == RestStatus.REQUEST_TIMEOUT.getStatus()) {
                        bulkRequestTimeoutErrors.increment();
                    } else if (status >= RestStatus.INTERNAL_SERVER_ERROR.getStatus()) {
                        bulkRequestServerErrors.increment();
                    } else { // Default to Bad Requests
                        bulkRequestBadErrors.increment();
                    }
                }
                handleRetriesAndFailures(bulkRequestForRetry, retryCount, backOffUtils, null, e);
                return;
            }
            if (bulkResponse.errors()) {
                handleRetriesAndFailures(bulkRequestForRetry, retryCount, backOffUtils, bulkResponse, null);
            } else {
                final int numberOfDocs = bulkRequestForRetry.getOperationsCount();
                if (retryCount == 1) {
                    sentDocumentsOnFirstAttemptCounter.increment(numberOfDocs);
                }
                sentDocumentsCounter.increment(bulkRequestForRetry.getOperationsCount());
                retryCountMap.remove(bulkRequestForRetry);
            }
        }
    }

    private AccumulatingBulkRequest<BulkOperation, BulkRequest> createBulkRequestForRetry(
            final AccumulatingBulkRequest<BulkOperation, BulkRequest> request, final BulkResponse response) {
        int newCount = retryCountMap.containsKey(request) ? (retryCountMap.get(request) + 1) : 1;
        if (response == null) {
            retryCountMap.put(request, newCount);
            // first attempt or retry due to Exception
            return request;
        } else {
            final AccumulatingBulkRequest requestToReissue = bulkRequestSupplier.get();
            if (request != requestToReissue) {
                retryCountMap.put(requestToReissue, newCount);
                retryCountMap.remove(request);
            }
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
