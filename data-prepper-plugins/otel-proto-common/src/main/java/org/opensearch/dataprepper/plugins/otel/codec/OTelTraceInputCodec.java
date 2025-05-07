package org.opensearch.dataprepper.plugins.otel.codec;

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;



public class OTelTraceInputCodec extends OTelProtoOpensearchCodec.OTelProtoDecoder implements InputCodec {

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        TracesData.Builder builder = TracesData.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(inputStream), builder);
        TracesData build = builder.build();
        List<ResourceSpans> resourceSpansList = build.getResourceSpansList();

        Instant timeReceived = Instant.now();
        resourceSpansList.stream()
                .flatMap(rs -> parseResourceSpans(rs, timeReceived).stream())
                .forEach(span -> eventConsumer.accept(new Record<>(span)));
    }
}

