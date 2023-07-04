/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.newline;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name = "newline", pluginType = OutputCodec.class)
public class NewlineDelimitedOutputCodec implements OutputCodec {

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        // TODO: implement
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        // TODO: get the event data and
        //  get the header record and message record and write event data to the outputstream
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        // TODO: Close the output stream
    }

    private void writeByteArrayToOutputStream(final OutputStream outputStream, final Object object) throws IOException {
        // TODO: common method to write byte array data to OutputStream
    }

    @Override
    public String getExtension() {
        return null;
    }
}
