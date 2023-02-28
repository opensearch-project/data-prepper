/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.codec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * A codec parsing data through an output stream. Each implementation of this class should
 * support parsing a specific type or format of data. See sub-classes for examples.
 */
public interface Codec {
    /**
     * Parses an {@link OutputStream}. Implementors should call the {@link Collection} for each
     * {@link Record} loaded from the {@link OutputStream}.
     *
     * @param outputStream   The output stream for the json data
     * @param eventCollection The collection which holds record events
     */
    void parse(OutputStream outputStream, Collection<Record<Event>> eventCollection) throws IOException;

    void parse(OutputStream outputStream, Record<Event> eventCollection) throws IOException;

    void parse(OutputStream outputStream, Event event) throws IOException;
}
