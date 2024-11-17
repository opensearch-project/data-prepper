/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssourcenew;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;

public interface SqsMessageHandler {
    void handleMessage(final Message message,
                        final BufferAccumulator<Record<Event>> bufferAccumulator,
                        final AcknowledgementSet acknowledgementSet) throws IOException ;
}
