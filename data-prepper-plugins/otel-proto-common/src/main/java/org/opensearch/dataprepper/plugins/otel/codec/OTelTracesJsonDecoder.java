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

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

public class OTelTracesJsonDecoder implements ByteDecoder {

    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;

    public OTelTracesJsonDecoder(OTelOutputFormat otelOutputFormat) {
        otelProtoDecoder = otelOutputFormat == OTelOutputFormat.OPENSEARCH
                ? new OTelProtoOpensearchCodec.OTelProtoDecoder()
                : new OTelProtoStandardCodec.OTelProtoDecoder();
    }

    @Override
    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        Reader reader = new InputStreamReader(inputStream);
        ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(reader, builder);
        ExportTraceServiceRequest request = builder.build();

        List<Span> spans = otelProtoDecoder.parseExportTraceServiceRequest(request, timeReceivedMs);
        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
    }
}
