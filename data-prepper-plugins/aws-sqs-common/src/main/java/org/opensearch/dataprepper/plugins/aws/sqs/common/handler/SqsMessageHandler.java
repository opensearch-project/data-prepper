/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.handler;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public interface SqsMessageHandler {
    List<DeleteMessageBatchRequestEntry> handleMessage(final List<Message> messages,
                                                       final AcknowledgementSet acknowledgementSet);
}
