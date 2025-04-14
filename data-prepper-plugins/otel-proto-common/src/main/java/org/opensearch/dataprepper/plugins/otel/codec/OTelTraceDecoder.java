
/*
 *  * Copyright OpenSearch Contributors
 *   * SPDX-License-Identifier: Apache-2.0
 *    */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.trace.Span;


import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.time.Instant;


public class OTelTraceDecoder implements ByteDecoder {
    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    public OTelTraceDecoder(OTelOutputFormat otelOutputFormat) {
        otelProtoDecoder = otelOutputFormat == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder();
    }
    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(inputStream);
        AtomicInteger droppedCounter = new AtomicInteger(0);
        List<Span> spans = otelProtoDecoder.parseExportTraceServiceRequest(request, timeReceivedMs);
        for (Span span: spans) {
            eventConsumer.accept(new Record<>(span));
        }
    }
}
