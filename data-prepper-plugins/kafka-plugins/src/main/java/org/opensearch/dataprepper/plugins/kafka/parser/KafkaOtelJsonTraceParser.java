package org.opensearch.dataprepper.plugins.kafka.parser;

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Parsing Open Telemetry trace json format from kafka topic to Spans.
 */
public class KafkaOtelJsonTraceParser extends OTelProtoOpensearchCodec.OTelProtoDecoder {

    public List<Span> parse(final String inputJson, final Instant receivedTimeStamp, final Consumer<Record<Event>> eventConsumer) throws IOException {

        List<Span> spans = parseSpans(inputJson, receivedTimeStamp);

        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
        return spans;
    }

    private List<Span> parseSpans(final String inputJson, final Instant timeReceived) throws IOException {
        TracesData.Builder builder = TracesData.newBuilder();
        JsonFormat.parser().merge(inputJson, builder);
        TracesData build = builder.build();
        List<ResourceSpans> resourceSpansList = build.getResourceSpansList();

        return resourceSpansList.stream()
                .flatMap(rs -> parseResourceSpans(rs, timeReceived).stream()).collect(Collectors.toList());
    }
}
