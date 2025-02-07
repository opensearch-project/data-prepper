/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.record.Record;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.nio.ByteBuffer;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.function.Consumer;
import java.time.Instant;
 
public class OTelLogsProtoBufDecoder implements ByteDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsProtoBufDecoder.class);
    private static final int MAX_REQUEST_LEN = (8 * 1024 * 1024);
    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    private final boolean lengthPrefixedEncoding;
    
    public OTelLogsProtoBufDecoder(boolean lengthPrefixedEncoding) {
        otelProtoDecoder = new OTelProtoCodec.OTelProtoDecoder();
        this.lengthPrefixedEncoding = lengthPrefixedEncoding;
    }

    private void parseRequest(byte[] buffer, final Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        ExportLogsServiceRequest request = ExportLogsServiceRequest.parseFrom(buffer);
        List<OpenTelemetryLog> logs = otelProtoDecoder.parseExportLogsServiceRequest(request, timeReceivedMs);
        for (OpenTelemetryLog log: logs) {
            eventConsumer.accept(new Record<>(log));
        }
    }

    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        Reader reader = new InputStreamReader(inputStream);
        ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        // As per the implementation of AWSS3 exporter in "otlp_proto" format at
        // https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/awss3exporter
        // Each request is written in a separate S3 object, so, no legth preceeding the actual data
        // Same with Kafka exporter too. Each message is written as a separate message to Kafka
        if (!lengthPrefixedEncoding) {
            if (inputStream.available() > MAX_REQUEST_LEN) {
                throw new IOException("buffer length exceeds max allowed buffer length of "+ MAX_REQUEST_LEN);
            }
            byte[] buffer = inputStream.readAllBytes();
            parseRequest(buffer, timeReceivedMs, eventConsumer);
            return;
        }
        // As per the implementation of File exporter in "proto" format at
        // https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/fileexporter/file_writer.go
        // The file input stream should have length, followed by the request of "length" bytes
        byte[] lenBytes = new byte[4];
        while (inputStream.read(lenBytes, 0, 4) == 4) {
            ByteBuffer lengthBuffer = ByteBuffer.wrap(lenBytes);
            int len = lengthBuffer.getInt();
            if (len > MAX_REQUEST_LEN) {
                throw new IOException("buffer length exceeds max allowed buffer length of "+ MAX_REQUEST_LEN);
            }
            byte[] buffer = new byte[len];
            if (inputStream.read(buffer, 0, len) != len) {
                LOG.warn("Failed to read {} bytes", len);
                continue;
            }
            parseRequest(buffer, timeReceivedMs, eventConsumer);
        }
    }
}

