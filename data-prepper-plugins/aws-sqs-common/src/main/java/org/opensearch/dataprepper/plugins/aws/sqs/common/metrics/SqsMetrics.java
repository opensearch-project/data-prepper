/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class SqsMetrics {

    public static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";

    public static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";

    public static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";

    public static final String SQS_MESSAGES_DELETE_FAILED_METRIC_NAME = "sqsMessagesDeleteFailed";

    public static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";

    public static final String ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Counter sqsMessagesDeleteFailedCounter;
    private final Counter acknowledgementSetCallbackCounter;
    private final Timer sqsMessageDelayTimer;

    public SqsMetrics(final PluginMetrics pluginMetrics){
        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        sqsMessagesDeleteFailedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETE_FAILED_METRIC_NAME);
        sqsMessageDelayTimer = pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME);
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME);
    }

    public Counter getSqsMessagesReceivedCounter() {
        return sqsMessagesReceivedCounter;
    }

    public Counter getSqsMessagesDeletedCounter() {
        return sqsMessagesDeletedCounter;
    }

    public Counter getSqsMessagesFailedCounter() {
        return sqsMessagesFailedCounter;
    }

    public Counter getSqsMessagesDeleteFailedCounter() {
        return sqsMessagesDeleteFailedCounter;
    }

    public Counter getAcknowledgementSetCallbackCounter() {
        return acknowledgementSetCallbackCounter;
    }

    public Timer getSqsMessageDelayTimer() {
        return sqsMessageDelayTimer;
    }
}
