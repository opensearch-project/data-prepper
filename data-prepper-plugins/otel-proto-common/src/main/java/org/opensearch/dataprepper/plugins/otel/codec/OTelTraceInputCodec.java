package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Consumer;


@DataPrepperPlugin(name = "otel_trace", pluginType = InputCodec.class, pluginConfigurationType = OTelTraceInputCodecConfig.class)
public class OTelTraceInputCodec extends OTelProtoOpensearchCodec.OTelProtoDecoder implements InputCodec {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceInputCodec.class);
    private final OTelTraceInputCodecConfig codecConfig;
    private final OtelTraceJsonDecoder decoder;

    @DataPrepperPluginConstructor
    public OTelTraceInputCodec(final OTelTraceInputCodecConfig codecConfig) {
        LOG.info("OTelTraceInputCodec initialization");
        this.codecConfig = codecConfig;
        decoder = new OtelTraceJsonDecoder();
    }

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        OTelFormatOption format = codecConfig.getFormat();

        if (format == OTelFormatOption.JSON) {
            decoder.parse(inputStream, Instant.now(), eventConsumer);
        }
    }
}

