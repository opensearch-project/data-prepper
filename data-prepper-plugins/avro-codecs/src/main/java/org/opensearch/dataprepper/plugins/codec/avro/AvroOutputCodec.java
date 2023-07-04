/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as AVRO Data
 */
@DataPrepperPlugin(name = "avro", pluginType = OutputCodec.class, pluginConfigurationType = AvroOutputCodecConfig.class)
public class AvroOutputCodec implements OutputCodec {

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        // TODO: initiate config
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        // TODO: do the initial wrapping
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream,final String tagsTargetKey) throws IOException {
        // TODO: write event data to the outputstream
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        // TODO: do the final wrapping like closing outputstream
    }

    @Override
    public String getExtension() {
        return null;
    }

    static Schema parseSchema(final String schema) {
        // TODO: generate schema from schema string and return
        return null;
    }

}


