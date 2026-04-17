/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.codec;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.opensearch.dataprepper.model.event.Event;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Encodes a Data Prepper Event into an OTLP ResourceLogs protobuf message.
 */
public class OtlpLogEncoder {

    private static final String SCOPE_NAME = "data-prepper-otlp-sink";

    public ResourceLogs encode(final Event event) {
        final long nowNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        final LogRecord.Builder logBuilder = LogRecord.newBuilder()
                .setObservedTimeUnixNano(nowNanos);

        final Instant eventTime = event.getMetadata() != null ? event.getMetadata().getTimeReceived() : null;
        if (eventTime != null) {
            logBuilder.setTimeUnixNano(TimeUnit.SECONDS.toNanos(eventTime.getEpochSecond()) + eventTime.getNano());
        } else {
            logBuilder.setTimeUnixNano(nowNanos);
        }

        logBuilder.setBody(AnyValue.newBuilder().setStringValue(event.toJsonString()).build());

        final Map<String, Object> eventData = event.toMap();
        for (final Map.Entry<String, Object> entry : eventData.entrySet()) {
            logBuilder.addAttributes(KeyValue.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(toAnyValue(entry.getValue()))
                    .build());
        }

        return ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .setScope(InstrumentationScope.newBuilder().setName(SCOPE_NAME).build())
                        .addLogRecords(logBuilder.build())
                        .build())
                .build();
    }

    private AnyValue toAnyValue(final Object value) {
        if (value == null) {
            return AnyValue.getDefaultInstance();
        }
        if (value instanceof String) {
            return AnyValue.newBuilder().setStringValue((String) value).build();
        }
        if (value instanceof Boolean) {
            return AnyValue.newBuilder().setBoolValue((Boolean) value).build();
        }
        if (value instanceof Integer || value instanceof Long) {
            return AnyValue.newBuilder().setIntValue(((Number) value).longValue()).build();
        }
        if (value instanceof Float || value instanceof Double) {
            return AnyValue.newBuilder().setDoubleValue(((Number) value).doubleValue()).build();
        }
        return AnyValue.newBuilder().setStringValue(value.toString()).build();
    }
}
