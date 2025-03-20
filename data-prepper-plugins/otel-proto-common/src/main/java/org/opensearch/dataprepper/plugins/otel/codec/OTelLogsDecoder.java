/*
 *  * Copyright OpenSearch Contributors
 *   * SPDX-License-Identifier: Apache-2.0
 *    */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.time.Instant;


public class OTelLogsDecoder implements ByteDecoder {
    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    public OTelLogsDecoder(OTelOutputFormat otelOutputFormat) {
        otelProtoDecoder = otelOutputFormat == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder();
    }
    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        ExportLogsServiceRequest request = ExportLogsServiceRequest.parseFrom(inputStream);
        AtomicInteger droppedCounter = new AtomicInteger(0);
        List<OpenTelemetryLog> logs = otelProtoDecoder.parseExportLogsServiceRequest(request, timeReceivedMs);
        for (OpenTelemetryLog log: logs) {
            eventConsumer.accept(new Record<>(log));
        }
    }

}
