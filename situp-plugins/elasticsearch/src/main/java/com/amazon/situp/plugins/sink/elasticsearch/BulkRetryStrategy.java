package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Set;
import java.util.function.Supplier;

public final class BulkRetryStrategy {
    private final Set<Integer> retryStatus;
    private final RequestFunction<BulkRequest, BulkResponse> requestFunction;
    private final Supplier<BulkRequest> bulkRequestSupplier;

    public BulkRetryStrategy(final RequestFunction<BulkRequest, BulkResponse> requestFunction,
                             final Supplier<BulkRequest> bulkRequestSupplier, final Set<Integer> retryStatus) {
        this.requestFunction = requestFunction;
        this.bulkRequestSupplier = bulkRequestSupplier;
        this.retryStatus = retryStatus;
    }

    public boolean canRetry(final BulkResponse response) {
        for (final BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
                if (retryStatus.contains(bulkItemResponse.status().getStatus())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean canRetry(final Exception e) {
        return (e instanceof RemoteTransportException &&
                retryStatus.contains(((RemoteTransportException) e).status().getStatus()));
    }

    public Tuple<BulkRequest, BulkResponse> retry(final BulkRequest request) throws Exception {
        final BulkResponse response = requestFunction.apply(request);
        return Tuple.tuple(request, response);
    }

    public Tuple<BulkRequest, BulkResponse> handleRetry(final BulkRequest request, final BulkResponse response)
            throws Exception {
        final RetryUtils retryUtils = new RetryUtils(BackoffPolicy.exponentialBackoff().iterator());
        Tuple<BulkRequest, BulkResponse> tuple = Tuple.tuple(request, response);
        BulkRequest bulkRequestForRetry = createBulkRequestForRetry(request, response);
        while (retryUtils.next()) {
            try {
                tuple = retry(bulkRequestForRetry);
            } catch (final Exception e) {
                if (retryUtils.hasNext()) {
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
                    requestToReissue.add(request.requests().get(index));
                }
                index++;
            }
            return requestToReissue;
        }
    }
}
