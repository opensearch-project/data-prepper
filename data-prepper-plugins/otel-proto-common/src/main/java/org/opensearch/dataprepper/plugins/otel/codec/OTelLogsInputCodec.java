/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "otel_logs", pluginType = InputCodec.class, pluginConfigurationType = OTelLogsInputCodecConfig.class)
public class OTelLogsInputCodec implements InputCodec {
    private final OTelLogsInputCodecConfig config;

    @DataPrepperPluginConstructor
    public OTelLogsInputCodec(final OTelLogsInputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }    
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        if (OTelLogsInputCodecConfig.JSON_FORMAT.equals(config.getFormat())) {
            OTelLogsJsonDecoder decoder = new OTelLogsJsonDecoder();
            decoder.parse(inputStream, null, eventConsumer);
        } 
    }      
}