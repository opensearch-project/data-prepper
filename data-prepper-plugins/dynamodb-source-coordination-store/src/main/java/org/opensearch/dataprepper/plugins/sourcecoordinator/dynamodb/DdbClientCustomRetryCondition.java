/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;

public class DdbClientCustomRetryCondition implements RetryCondition {
    private static final Logger LOG = LoggerFactory.getLogger(DdbClientCustomRetryCondition.class);
    private static final int DEFAULT_RETRIES = 10;

    private final RetryCondition defaultRetryCondition;

    public DdbClientCustomRetryCondition() {
        this.defaultRetryCondition = RetryCondition.defaultRetryCondition();
    }

    @Override
    public boolean shouldRetry(final RetryPolicyContext context) {

        if (context.exception() instanceof ProvisionedThroughputExceededException) {
            LOG.warn("Received throttling from DynamoDb after {} attempts, retrying: {}", context.retriesAttempted(), context.exception().getMessage());
            return true;
        }

        if (context.retriesAttempted() >= DEFAULT_RETRIES) {
            return false;
        }

        return defaultRetryCondition.shouldRetry(context);
    }
}
