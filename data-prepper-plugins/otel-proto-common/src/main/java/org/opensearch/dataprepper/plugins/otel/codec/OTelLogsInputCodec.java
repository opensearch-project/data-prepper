/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "otel_logs", pluginType = InputCodec.class, pluginConfigurationType = OTelLogsInputCodecConfig.class)
public class OTelLogsInputCodec implements InputCodec {
    private final ByteDecoder decoder;

    @DataPrepperPluginConstructor
    public OTelLogsInputCodec(final OTelLogsInputCodecConfig config) {
        Objects.requireNonNull(config);
        OTelLogsFormatOption format = config.getFormat();
        OTelOutputFormat otelFormat = config.getOTelOutputFormat();
        if (format == OTelLogsFormatOption.JSON) {
            decoder = new OTelLogsJsonDecoder(otelFormat);
        } else if (format == OTelLogsFormatOption.PROTOBUF) {
            decoder = new OTelLogsProtoBufDecoder(otelFormat, config.getLengthPrefixedEncoding());
        } else {
            throw new RuntimeException("The codec " + config.getFormat() + " is not supported.");
        }
    }
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        decoder.parse(inputStream, null, eventConsumer);
    }
}
