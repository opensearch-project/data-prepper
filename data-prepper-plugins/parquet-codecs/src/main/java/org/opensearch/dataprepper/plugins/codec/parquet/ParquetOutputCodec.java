/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;


import org.apache.avro.Schema;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;


@DataPrepperPlugin(name = "parquet", pluginType = OutputCodec.class, pluginConfigurationType = ParquetOutputCodecConfig.class)
public class ParquetOutputCodec implements OutputCodec {

    @DataPrepperPluginConstructor
    public ParquetOutputCodec(final ParquetOutputCodecConfig config) {
        // TODO: implement initiate config
    }


    @Override
    public void start(final OutputStream outputStream) throws IOException {
        // TODO: do the initial wrapping
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        // TODO: Close the output stream
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        // TODO: get the event data and write in output stream
    }

    @Override
    public String getExtension() {
        return null;
    }

    static Schema parseSchema(final String schemaString) {
        return null;
    }

}