/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "otel_traces", pluginType = InputCodec.class, pluginConfigurationType = OTelTracesInputCodecConfig.class)
public class OTelTracesInputCodec implements InputCodec {

    private final ByteDecoder decoder;

    @DataPrepperPluginConstructor
    public OTelTracesInputCodec(final OTelTracesInputCodecConfig config) {
        Objects.requireNonNull(config);
        OTelTracesFormatOption format = config.getFormat();
        OTelOutputFormat otelFormat = config.getOTelOutputFormat();

        if (format == OTelTracesFormatOption.JSON) {
            decoder = new OTelTracesJsonDecoder(otelFormat);
        } else if (format == OTelTracesFormatOption.PROTOBUF) {
            decoder = new OTelTracesProtoBufDecoder(otelFormat, config.getLengthPrefixedEncoding());
        } else {
            throw new RuntimeException("The format " + config.getFormat() + " is not supported.");
        }
    }

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        decoder.parse(inputStream, null, eventConsumer);
    }
}
