/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.handler;

import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public interface SqsMessageHandler {
    void handleMessage(final List<Message> messages, final String queueUrl);
}
