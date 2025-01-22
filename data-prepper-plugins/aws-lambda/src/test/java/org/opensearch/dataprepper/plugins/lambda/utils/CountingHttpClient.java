package org.opensearch.dataprepper.plugins.lambda.utils;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingHttpClient implements SdkAsyncHttpClient {
    private final SdkAsyncHttpClient delegate;
    private final AtomicInteger requestCount = new AtomicInteger(0);

    public CountingHttpClient(SdkAsyncHttpClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
        requestCount.incrementAndGet();
        return delegate.execute(request);
    }

    @Override
    public void close() {
        delegate.close();
    }

    public int getRequestCount() {
        return requestCount.get();
    }
}

