/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.record.Record;

import com.google.protobuf.util.JsonFormat;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
 
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.function.Consumer;
import java.time.Instant;
 
public class OTelLogsJsonDecoder implements ByteDecoder {
    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    
    public OTelLogsJsonDecoder() {
        otelProtoDecoder = new OTelProtoCodec.OTelProtoDecoder();
    }

    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        Reader reader = new InputStreamReader(inputStream);
        ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        JsonFormat.parser().merge(reader, builder);
        ExportLogsServiceRequest request = builder.build(); 
        List<OpenTelemetryLog> logs = otelProtoDecoder.parseExportLogsServiceRequest(request, timeReceivedMs);
        for (OpenTelemetryLog log: logs) {
            eventConsumer.accept(new Record<>(log));
        }
    }
}