package org.opensearch.dataprepper.plugins.lambda.common.util;

import software.amazon.awssdk.core.retry.RetryPolicyContext;

import java.util.concurrent.atomic.AtomicInteger;

//Used ONLY for tests
public class CountingRetryCondition extends CustomLambdaRetryCondition {
    private final AtomicInteger retryCount = new AtomicInteger(0);

    @Override
    public boolean shouldRetry(RetryPolicyContext context) {
        boolean shouldRetry = super.shouldRetry(context);
        if (shouldRetry) {
            retryCount.incrementAndGet();
        }
        return shouldRetry;
    }

    public int getRetryCount() {
        return retryCount.get();
    }
}