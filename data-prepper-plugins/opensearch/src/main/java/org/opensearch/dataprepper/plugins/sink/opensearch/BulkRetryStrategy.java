/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.linecorp.armeria.client.retry.Backoff;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.Counter;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperation;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    static final long INITIAL_DELAY_MS = 50;
    static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();

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

    private final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction;
    private final BiConsumer<List<FailedBulkOperation>, Throwable> logFailure;
    private final PluginMetrics pluginMetrics;
    private final Supplier<AccumulatingBulkRequest> bulkRequestSupplier;
    private final int maxRetries;
    private final String pluginId;
    private final String pluginName;
    private final String pipelineName;
    private final ObjectMapper objectMapper;

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
    private static final Logger LOG = LoggerFactory.getLogger(BulkRetryStrategy.class);

    static class BulkOperationRequestResponse {
        final AccumulatingBulkRequest bulkRequest;
        final BulkResponse response;
        public BulkOperationRequestResponse(final AccumulatingBulkRequest bulkRequest, final BulkResponse response) {
            this.bulkRequest = bulkRequest;
            this.response = response;
        }
        AccumulatingBulkRequest getBulkRequest() {
            return bulkRequest;
        }
        BulkResponse getResponse() {
            return response;
        }
    }

    public BulkRetryStrategy(final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction,
                             final BiConsumer<List<FailedBulkOperation>, Throwable> logFailure,
                             final PluginMetrics pluginMetrics,
                             final int maxRetries,
                             final Supplier<AccumulatingBulkRequest> bulkRequestSupplier,
                             final PluginSetting pluginSetting) {
        this.requestFunction = requestFunction;
        this.logFailure = logFailure;
        this.pluginMetrics = pluginMetrics;
        this.bulkRequestSupplier = bulkRequestSupplier;
        this.maxRetries = maxRetries;
        this.pipelineName = pluginSetting.getPipelineName();
        this.pluginId = pluginSetting.getName();
        this.pluginName = pluginSetting.getName();
        this.objectMapper = new ObjectMapper();

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

    private void incrementErrorCounters(final Exception e) {
        if (e instanceof OpenSearchException) {
            int status = ((OpenSearchException) e).status();
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
    }

    public void execute(final AccumulatingBulkRequest bulkRequest) throws InterruptedException {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS).withMaxAttempts(maxRetries);
        BulkOperationRequestResponse operationResponse;
        BulkResponse response = null;
        AccumulatingBulkRequest request = bulkRequest;
        int attempt = 1;
        do {
            operationResponse = handleRetry(request, response, attempt);
            if (operationResponse != null) {
                final long delayMillis = backoff.nextDelayMillis(attempt++);
                request = operationResponse.getBulkRequest();
                response = operationResponse.getResponse();
                if (delayMillis < 0) {
                    RuntimeException e = new RuntimeException(String.format("Number of retries reached the limit of max retries (configured value %d)", maxRetries));
                    handleFailures(request, null, e);
                    break;
                }
                // Wait for backOff duration
                try {
                    Thread.sleep(delayMillis);
                } catch (final InterruptedException e){
                    LOG.error("Thread is interrupted while attempting to bulk write to OpenSearch with retry.", e);
                }
            }
        } while (operationResponse != null);
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
                        !NON_RETRY_STATUS.contains(((OpenSearchException) e).status())));
    }

    private BulkOperationRequestResponse handleRetriesAndFailures(final AccumulatingBulkRequest bulkRequestForRetry,
                                          final int retryCount,
                                          final BulkResponse bulkResponse,
                                          Exception e) throws InterruptedException {
        final boolean doRetry = (Objects.isNull(e)) ? canRetry(bulkResponse) : canRetry(e);
        if (!Objects.isNull(bulkResponse) && retryCount == 1) { // first attempt
            for (final BulkResponseItem bulkItemResponse : bulkResponse.items()) {
                if (bulkItemResponse.error() == null) {
                    sentDocumentsOnFirstAttemptCounter.increment();
                }
            }
        }
        if (doRetry) {
            if (retryCount % 5 == 0) {
                LOG.warn("Bulk Operation Failed. Number of retries {}. Retrying... ", retryCount, e);
            }
            bulkRequestNumberOfRetries.increment();
            return new BulkOperationRequestResponse(bulkRequestForRetry, bulkResponse);
        } else {
            handleFailures(bulkRequestForRetry, bulkResponse, e);
        }
        return null;
    }

    private void handleFailures(final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest, final BulkResponse bulkResponse, final Throwable failure) {
        if (Objects.isNull(failure)) {
            handleFailures(bulkRequest, bulkResponse.items());
        } else {
            handleFailures(bulkRequest, failure);
        }
        bulkRequestFailedCounter.increment();
    }

    private BulkOperationRequestResponse handleRetry(final AccumulatingBulkRequest request, final BulkResponse response, int retryCount) throws InterruptedException {
        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequestForRetry = createBulkRequestForRetry(request, response);
        final BulkResponse bulkResponse;
        try {
            bulkResponse = requestFunction.apply(bulkRequestForRetry);
        } catch (Exception e) {
            incrementErrorCounters(e);
            return handleRetriesAndFailures(bulkRequestForRetry, retryCount, null, e);
        }
        if (bulkResponse.errors()) {
            return handleRetriesAndFailures(bulkRequestForRetry, retryCount, bulkResponse, null);
        } else {
            final int numberOfDocs = bulkRequestForRetry.getOperationsCount();
            final boolean firstAttempt = (retryCount == 1);
            if (firstAttempt) {
                sentDocumentsOnFirstAttemptCounter.increment(numberOfDocs);
            }
            sentDocumentsCounter.increment(bulkRequestForRetry.getOperationsCount());
            for (final BulkOperationWrapper bulkOperation: bulkRequestForRetry.getOperations()) {
                bulkOperation.releaseEventHandle(true);
            }
        }
        return null;
    }

    private AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> createBulkRequestForRetry(
            final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> request, final BulkResponse response) {
        if (response == null) {
            // first attempt or retry due to Exception
            return request;
        } else {
            final AccumulatingBulkRequest requestToReissue = bulkRequestSupplier.get();
            final ImmutableList.Builder<FailedBulkOperation> nonRetryableFailures = ImmutableList.builder();
            int index = 0;
            for (final BulkResponseItem bulkItemResponse : response.items()) {
                BulkOperationWrapper bulkOperation =
                    (BulkOperationWrapper)request.getOperationAt(index);
                if (bulkItemResponse.error() != null) {
                    if (!NON_RETRY_STATUS.contains(bulkItemResponse.status())) {
                        requestToReissue.addOperation(bulkOperation);
                    } else {
                        nonRetryableFailures.add(FailedBulkOperation.builder()
                                .withBulkOperation(bulkOperation)
                                .withBulkResponseItem(bulkItemResponse)
                                .build());
                        documentErrorsCounter.increment();
                    }
                } else {
                    sentDocumentsCounter.increment();
                    bulkOperation.releaseEventHandle(true);
                }
                index++;
            }
            logFailure.accept(nonRetryableFailures.build(), null);
            return requestToReissue;
        }
    }

    private void handleFailures(final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> accumulatingBulkRequest, final List<BulkResponseItem> itemResponses) {
        assert accumulatingBulkRequest.getOperationsCount() == itemResponses.size();
        final ImmutableList.Builder<FailedBulkOperation> failures = ImmutableList.builder();
        for (int i = 0; i < itemResponses.size(); i++) {
            final BulkResponseItem bulkItemResponse = itemResponses.get(i);
            final BulkOperationWrapper bulkOperation = accumulatingBulkRequest.getOperationAt(i);
            if (bulkItemResponse.error() != null) {
                failures.add(FailedBulkOperation.builder()
                    .withBulkOperation(bulkOperation)
                    .withBulkResponseItem(bulkItemResponse)
                    .build());
                documentErrorsCounter.increment();
            } else {
                sentDocumentsCounter.increment();
                bulkOperation.releaseEventHandle(true);
            }
        }
        logFailure.accept(failures.build(), null);
    }

    private void handleFailures(final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> accumulatingBulkRequest, final Throwable failure) {
        documentErrorsCounter.increment(accumulatingBulkRequest.getOperationsCount());
        final ImmutableList.Builder<FailedBulkOperation> failures = ImmutableList.builder();
        final List<BulkOperationWrapper> bulkOperations = accumulatingBulkRequest.getOperations();
        for (int i = 0; i < bulkOperations.size(); i++) {
            final BulkOperationWrapper bulkOperation = bulkOperations.get(i);
            failures.add(FailedBulkOperation.builder()
                    .withBulkOperation(bulkOperation)
                    .withFailure(failure)
                    .build());
        }

        logFailure.accept(failures.build(), failure);
    }

}
