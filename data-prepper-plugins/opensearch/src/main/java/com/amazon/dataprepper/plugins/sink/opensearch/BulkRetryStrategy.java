/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import io.micrometer.core.instrument.Counter;
import org.opensearch.OpenSearchException;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.amazon.dataprepper.plugins.sink.opensearch.ErrorCauseStringCreator.toSingleLineDisplayString;

public final class BulkRetryStrategy {
    public static final String DOCUMENTS_SUCCESS = "documentsSuccess";
    public static final String DOCUMENTS_SUCCESS_FIRST_ATTEMPT = "documentsSuccessFirstAttempt";
    public static final String DOCUMENT_ERRORS = "documentErrors";

    private static final Set<Integer> NON_RETRY_STATUS = new HashSet<>(
            Arrays.asList(
                    RestStatus.BAD_REQUEST.getStatus(),
                    RestStatus.NOT_FOUND.getStatus(),
                    RestStatus.CONFLICT.getStatus()
            ));

    private final RequestFunction<AccumulatingBulkRequest<BulkOperation, BulkRequest>, BulkResponse> requestFunction;
    private final BiConsumer<BulkOperation, Throwable> logFailure;
    private final PluginMetrics pluginMetrics;
    private final Supplier<AccumulatingBulkRequest> bulkRequestSupplier;

    private final Counter sentDocumentsCounter;
    private final Counter sentDocumentsOnFirstAttemptCounter;
    private final Counter documentErrorsCounter;

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
                if (canRetry(e)) {
                    handleRetry(bulkRequestForRetry, null, backOffUtils, false);
                } else {
                    handleFailures(bulkRequestForRetry, e);
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
