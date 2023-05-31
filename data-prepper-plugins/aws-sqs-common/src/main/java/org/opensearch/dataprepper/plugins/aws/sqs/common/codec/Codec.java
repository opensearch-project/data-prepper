/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws.sqs.common.codec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;

/**
 * Codec parses the content of SQS message into custom Java type.
 * <p>
 */
public interface Codec {
    /**
     * parse the request into custom type
     *
     * @param message is sqs message
     * @return The Record Event
     */
    Record<Event> parse(final String message) throws IOException;
}
