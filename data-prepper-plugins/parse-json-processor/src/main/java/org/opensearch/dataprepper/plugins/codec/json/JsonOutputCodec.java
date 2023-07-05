/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as JSON Data
 */
@DataPrepperPlugin(name = "json", pluginType = OutputCodec.class)
public class JsonOutputCodec implements OutputCodec {

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        // TODO: do the initial wrapping like start the array
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        // TODO: get the event data and write event data to the outputstream
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        // TODO: do the final wrapping like closing outputstream and close generator
    }

    @Override
    public String getExtension() {
        return null;
    }
}
