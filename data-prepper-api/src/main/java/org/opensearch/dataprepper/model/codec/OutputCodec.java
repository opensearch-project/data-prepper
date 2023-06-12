/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.Sink;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputCodec {

    /**
     * this method get called from {@link Sink} to do initial wrapping in {@link OutputStream}
     * Implementors should do initial wrapping according to the implementation
     *
     * @param outputStream outputStream param for wrapping
     * @throws IOException throws IOException when invalid input is received or not able to create wrapping
     */
    void start(OutputStream outputStream) throws IOException;

    /**
     * this method get called from {@link Sink} to write event in {@link OutputStream}
     * Implementors should do get data from event and write to the {@link OutputStream}
     *
     * @param event        event Record event
     * @param outputStream outputStream param to hold the event data
     * @throws IOException throws IOException when not able to write data to {@link OutputStream}
     */
    void writeEvent(Event event, OutputStream outputStream) throws IOException;

    /**
     * this method get called from {@link Sink} to do final wrapping in {@link OutputStream}
     * Implementors should do final wrapping according to the implementation
     *
     * @param outputStream outputStream param for wrapping
     * @throws IOException throws IOException when invalid input is received or not able to create wrapping
     */
    void complete(OutputStream outputStream) throws IOException;

    /**
     * used to get extension of file
     *
     * @return String
     */
    String getExtension();
}
