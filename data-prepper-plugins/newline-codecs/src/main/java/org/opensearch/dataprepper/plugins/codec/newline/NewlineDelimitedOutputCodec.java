/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.newline;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class NewlineDelimitedOutputCodec implements OutputCodec {
    @Override
    public void start(OutputStream outputStream) {
        Objects.requireNonNull(outputStream);
    }

    @Override
    public void complete(OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    @Override
    public void writeEvent(Event event, OutputStream outputStream) throws IOException {
        final byte[] byteArr = event.toJsonString().getBytes();
        outputStream.write(byteArr);
        outputStream.write("\n".getBytes());
    }

    @Override
    public String getExtension() {
        return null;
    }
}
