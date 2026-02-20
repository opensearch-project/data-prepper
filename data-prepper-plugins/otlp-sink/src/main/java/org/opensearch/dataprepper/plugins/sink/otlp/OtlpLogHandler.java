/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler for OTLP log signals.
 */
public class OtlpLogHandler implements OtlpSignalHandler {
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;

    public OtlpLogHandler(final OTelProtoStandardCodec.OTelProtoEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public Object encodeEvent(final Event event) throws Exception {
        return encoder.convertToResourceLogs((Log) event);
    }

    @Override
    public long getSerializedSize(final Object encodedData) {
        return ((ResourceLogs) encodedData).getSerializedSize();
    }

    @Override
    public byte[] buildRequestPayload(final List<Pair<Object, EventHandle>> batch) {
        final ExportLogsServiceRequest request = ExportLogsServiceRequest.newBuilder()
                .addAllResourceLogs(batch.stream()
                        .map(p -> (ResourceLogs) p.left())
                        .collect(Collectors.toList()))
                .build();
        return request.toByteArray();
    }

    @Override
    public Pair<Long, String> parsePartialSuccess(final byte[] responseBytes) throws Exception {
        final ExportLogsServiceResponse response = ExportLogsServiceResponse.parseFrom(responseBytes);
        if (response.hasPartialSuccess()) {
            final long rejectedCount = response.getPartialSuccess().getRejectedLogRecords();
            final String errorMessage = response.getPartialSuccess().getErrorMessage();
            return Pair.of(rejectedCount, errorMessage);
        }
        return Pair.of(0L, "");
    }
}
