/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.elasticsearch;

import com.amazon.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.Counter;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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

    private final RequestFunction<BulkRequest, BulkResponse> requestFunction;
    private final BiConsumer<DocWriteRequest<?>, Throwable> logFailure;
    private final PluginMetrics pluginMetrics;
    private final Supplier<BulkRequest> bulkRequestSupplier;

    private final Counter sentDocumentsCounter;
    private final Counter sentDocumentsOnFirstAttemptCounter;
    private final Counter documentErrorsCounter;

    public BulkRetryStrategy(final RequestFunction<BulkRequest, BulkResponse> requestFunction,
                             final BiConsumer<DocWriteRequest<?>, Throwable> logFailure,
                             final PluginMetrics pluginMetrics,
                             final Supplier<BulkRequest> bulkRequestSupplier) {
        this.requestFunction = requestFunction;
        this.logFailure = logFailure;
        this.pluginMetrics = pluginMetrics;
        this.bulkRequestSupplier = bulkRequestSupplier;

        sentDocumentsCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS);
        sentDocumentsOnFirstAttemptCounter = pluginMetrics.counter(DOCUMENTS_SUCCESS_FIRST_ATTEMPT);
        documentErrorsCounter = pluginMetrics.counter(DOCUMENT_ERRORS);
    }

    public void execute(final BulkRequest bulkRequest) throws InterruptedException {
        // Exponential backoff run forever
        // TODO: replace with custom backoff policy setting including maximum interval between retries
        final BackOffUtils backOffUtils = new BackOffUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), Integer.MAX_VALUE).iterator());
        handleRetry(bulkRequest, null, backOffUtils, true);
    }

    public boolean canRetry(final BulkResponse response) {
        for (final BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed() && !NON_RETRY_STATUS.contains(bulkItemResponse.status().getStatus())) {
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

    private void handleRetry(final BulkRequest request, final BulkResponse response,
                             final BackOffUtils backOffUtils, final boolean firstAttempt) throws InterruptedException {
        final BulkRequest bulkRequestForRetry = createBulkRequestForRetry(request, response);
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
                    handleFailures(bulkRequestForRetry.requests(), e);
                }

                return;
            }
            if (bulkResponse.hasFailures()) {
                if (canRetry(bulkResponse)) {
                    if (firstAttempt) {
                        for (final BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                            if (!bulkItemResponse.isFailed()) {
                                sentDocumentsOnFirstAttemptCounter.increment();
                            }
                        }
                    }
                    handleRetry(bulkRequestForRetry, bulkResponse, backOffUtils, false);
                } else {
                    handleFailures(bulkRequestForRetry.requests(), bulkResponse.getItems());
                }
            } else {
                final int numberOfDocs = bulkRequestForRetry.numberOfActions();
                if (firstAttempt) {
                    sentDocumentsOnFirstAttemptCounter.increment(numberOfDocs);
                }
                sentDocumentsCounter.increment(bulkRequestForRetry.numberOfActions());
            }
        }
    }

    private BulkRequest createBulkRequestForRetry(
            final BulkRequest request, final BulkResponse response) {
        if (response == null) {
            // first attempt or retry due to Exception
            return request;
        } else {
            final BulkRequest requestToReissue = bulkRequestSupplier.get();
            int index = 0;
            for (final BulkItemResponse bulkItemResponse : response.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    if (!NON_RETRY_STATUS.contains(bulkItemResponse.status().getStatus())) {
                        requestToReissue.add(request.requests().get(index));
                    } else {
                        // log non-retryable failed request
                        logFailure.accept(request.requests().get(index), bulkItemResponse.getFailure().getCause());
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

    private void handleFailures(final List<DocWriteRequest<?>> docWriteRequests, final BulkItemResponse[] itemResponses) {
        assert docWriteRequests.size() == itemResponses.length;
        for (int i = 0; i < itemResponses.length; i++) {
            final BulkItemResponse bulkItemResponse = itemResponses[i];
            final DocWriteRequest<?> docWriteRequest = docWriteRequests.get(i);
            if (bulkItemResponse.isFailed()) {
                final BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                logFailure.accept(docWriteRequest, failure.getCause());
                documentErrorsCounter.increment();
            } else {
                sentDocumentsCounter.increment();
            }
        }
    }

    private void handleFailures(final List<DocWriteRequest<?>> docWriteRequests, final Throwable failure) {
        documentErrorsCounter.increment(docWriteRequests.size());
        for (final DocWriteRequest<?> docWriteRequest: docWriteRequests) {
            logFailure.accept(docWriteRequest, failure);
        }
    }
}
