/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

/**
 * Retries CloudWatch Logs throttling and server errors.
 */
class CloudWatchLogsRetryCondition implements RetryCondition {

    /**
     * Returns true when CloudWatch Logs throttles the request or returns a 5XX status code.
     */
    @Override
    public boolean shouldRetry(final RetryPolicyContext context) {
        return isRetryable(context.exception());
    }

    /**
     * Returns true for a throttling exception or an AWS service exception with a 5XX status code.
     */
    static boolean isRetryable(final Throwable exception) {
        if (!(exception instanceof AwsServiceException)) {
            return false;
        }

        final AwsServiceException awsServiceException = (AwsServiceException) exception;
        final int statusCode = awsServiceException.statusCode();
        return awsServiceException.isThrottlingException()
                || (statusCode >= 500 && statusCode < 600);
    }
}
