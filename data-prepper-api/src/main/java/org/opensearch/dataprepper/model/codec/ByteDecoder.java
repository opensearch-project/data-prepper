/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.function.Consumer;
import java.time.Instant;

public interface ByteDecoder extends Serializable {
    /**
     * Parses an {@link InputStream}. Implementors should call the {@link Consumer} for each
     * {@link Record} loaded from the {@link InputStream}.
     *
     * @param inputStream   The input stream for code to process
     * @param timeReceived  The time received value to be populated in the Record
     * @param eventConsumer The consumer which handles each event from the stream
     * @throws IOException throws IOException when invalid input is received or incorrect codec name is provided
     */
    void parse(InputStream inputStream, Instant timeReceived, Consumer<Record<Event>> eventConsumer) throws IOException;
    
}
