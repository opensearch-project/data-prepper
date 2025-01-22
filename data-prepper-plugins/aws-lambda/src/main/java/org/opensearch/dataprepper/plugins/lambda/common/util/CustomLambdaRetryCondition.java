package org.opensearch.dataprepper.plugins.lambda.common.util;

import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.RetryPolicyContext;

public class CustomLambdaRetryCondition implements RetryCondition {

    @Override
    public boolean shouldRetry(RetryPolicyContext context) {
        Throwable exception = context.exception();
        if (exception != null) {
            return LambdaRetryStrategy.isRetryableException(exception);
        }

        return false;
    }
}
