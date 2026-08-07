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

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

public class OTelTracesProtoBufDecoder implements ByteDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(OTelTracesProtoBufDecoder.class);
    private static final int MAX_REQUEST_LEN = (8 * 1024 * 1024);

    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    private final boolean lengthPrefixedEncoding;

    public OTelTracesProtoBufDecoder(OTelOutputFormat otelOutputFormat, boolean lengthPrefixedEncoding) {
        otelProtoDecoder = otelOutputFormat == OTelOutputFormat.OPENSEARCH
                ? new OTelProtoOpensearchCodec.OTelProtoDecoder()
                : new OTelProtoStandardCodec.OTelProtoDecoder();
        this.lengthPrefixedEncoding = lengthPrefixedEncoding;
    }

    private void parseRequest(final ExportTraceServiceRequest request, final Instant timeReceivedMs,
                              Consumer<Record<Event>> eventConsumer) {
        List<Span> spans = otelProtoDecoder.parseExportTraceServiceRequest(request, timeReceivedMs);
        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
    }

    @Override
    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        if (!lengthPrefixedEncoding) {
            int available = inputStream.available();
            if (available > MAX_REQUEST_LEN) {
                throw new IllegalArgumentException(
                        String.format("Buffer length %d exceeds max allowed buffer length of %d", available, MAX_REQUEST_LEN));
            }
            ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(inputStream);
            parseRequest(request, timeReceivedMs, eventConsumer);
            return;
        }

        byte[] lenBytes = new byte[4];
        while (inputStream.read(lenBytes, 0, 4) == 4) {
            ByteBuffer lengthBuffer = ByteBuffer.wrap(lenBytes);
            int len = lengthBuffer.getInt();
            if (len > MAX_REQUEST_LEN) {
                throw new IllegalArgumentException(
                        String.format("Buffer length %d exceeds max allowed buffer length of %d", len, MAX_REQUEST_LEN));
            }
            byte[] buffer = new byte[len];
            if (inputStream.read(buffer, 0, len) != len) {
                LOG.warn("Failed to read {} bytes", len);
                continue;
            }
            ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(buffer);
            parseRequest(request, timeReceivedMs, eventConsumer);
        }
    }
}
