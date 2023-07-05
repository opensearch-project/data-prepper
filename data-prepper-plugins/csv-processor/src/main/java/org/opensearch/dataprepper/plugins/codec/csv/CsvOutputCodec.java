/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as CSV Data
 */
@DataPrepperPlugin(name = "csv", pluginType = OutputCodec.class, pluginConfigurationType = CsvOutputCodecConfig.class)
public class CsvOutputCodec implements OutputCodec {

    @DataPrepperPluginConstructor
    public CsvOutputCodec(final CsvOutputCodecConfig config) {
        // TODO: initiate config
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        // TODO: do the initial wrapping like get header and delimiter and write to Outputstream
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        // TODO: validate data according to header and write event data to the outputstream
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        // TODO: do the final wrapping like closing outputstream
    }

    private void writeByteArrayToOutputStream(final OutputStream outputStream, final byte[] byteArr) throws IOException {
        // TODO: common method to write byte array data to OutputStream
    }

    @Override
    public String getExtension() {
        return null;
    }
}
