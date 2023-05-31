/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputCodec {
    /**
     * converts a data-prepper {@link Event} into x-format of data to be loaded into any sink
     *
     * @param outputStream   Underlying stream into which Data-prepper events are written into
     * @throws IOException throws IOException when invalid or incompatible event comes up
     */

    void start(OutputStream outputStream);

    void complete(OutputStream outputStream) throws IOException;

    void writeEvent(Event event, OutputStream outputStream) throws IOException;

    String getExtension();
}
