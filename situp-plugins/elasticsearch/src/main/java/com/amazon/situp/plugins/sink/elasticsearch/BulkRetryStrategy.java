package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class BulkRetryStrategy {
    private static final Set<Integer> NON_RETRY_STATUS = new HashSet<>(
            Arrays.asList(
                    RestStatus.BAD_REQUEST.getStatus(),
                    RestStatus.NOT_FOUND.getStatus(),
                    RestStatus.CONFLICT.getStatus()
            ));

    private final RequestFunction<BulkRequest, BulkResponse> requestFunction;
    private final BiConsumer<DocWriteRequest<?>, Throwable> logFailure;
    private final Supplier<BulkRequest> bulkRequestSupplier;

    public BulkRetryStrategy(final RequestFunction<BulkRequest, BulkResponse> requestFunction,
                             final BiConsumer<DocWriteRequest<?>, Throwable> logFailure,
                             final Supplier<BulkRequest> bulkRequestSupplier) {
        this.requestFunction = requestFunction;
        this.logFailure = logFailure;
        this.bulkRequestSupplier = bulkRequestSupplier;
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
                (e instanceof ElasticsearchException &&
                        !NON_RETRY_STATUS.contains(((ElasticsearchException) e).status().getStatus())));
    }

    private Tuple<BulkRequest, BulkResponse> retry(final BulkRequest request) throws Exception {
        final BulkResponse response = requestFunction.apply(request);
        return Tuple.tuple(request, response);
    }

    public Tuple<BulkRequest, BulkResponse> handleRetry(final BulkRequest request, final BulkResponse response)
            throws Exception {
        // Exponential backoff run forever
        // TODO: replace with custom backoff policy setting including maximum interval between retries
        final BackOffUtils backOffUtils = new BackOffUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), Integer.MAX_VALUE).iterator());
        Tuple<BulkRequest, BulkResponse> tuple = Tuple.tuple(request, response);
        BulkRequest bulkRequestForRetry = createBulkRequestForRetry(request, response);
        while (backOffUtils.next()) {
            try {
                tuple = retry(bulkRequestForRetry);
            } catch (final Exception e) {
                if (backOffUtils.hasNext()) {
                    continue;
                } else {
                    throw e;
                }
            }
            if (!tuple.v2().hasFailures()) {
                return tuple;
            } else {
                bulkRequestForRetry = createBulkRequestForRetry(tuple.v1(), tuple.v2());
            }
        }
        return tuple;
    }

    private BulkRequest createBulkRequestForRetry(final BulkRequest request, final BulkResponse response) {
        if (response == null) {
            // retry due to Exception
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
                    }
                }
                index++;
            }
            return requestToReissue;
        }
    }
}
