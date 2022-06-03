/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * A codec parsing data through an input stream. Each implementation of this class should
 * support parsing a specific type or format of data. See sub-classes for examples.
 */
public interface Codec {
    /**
     * Parses an {@link InputStream}. Implementors should call the {@link Consumer} for each
     * {@link Record} loaded from the {@link InputStream}.
     *
     * @param inputStream   The input stream for the S3 object
     * @param eventConsumer The consumer which handles each event from the stream
     */
    void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException;
}
