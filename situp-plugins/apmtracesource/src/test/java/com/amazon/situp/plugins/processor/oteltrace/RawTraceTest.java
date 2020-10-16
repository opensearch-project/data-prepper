package com.amazon.situp.plugins.processor.oteltrace;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class RawTraceTest {

    @Test
    public void testBuildFromProto() {

        final long time = System.nanoTime();
        ByteBuffer traceIdBuffer = ByteBuffer.allocate(2 * Long.BYTES);
        traceIdBuffer.putLong(1);
        traceIdBuffer.putLong(2);
        ByteBuffer spanIdBuffer = ByteBuffer.allocate(Long.BYTES);
        spanIdBuffer.putLong(3);
        final String name = "LoggingController.save";
        final String instrumentationName = "instrumentation";
        final String instrumentationVersion = "instrumentationVersion";

        Resource resource = Resource.newBuilder()
                .addAttributes(0, KeyValue.newBuilder()
                        .setKey("telemetry.sdk.language")
                        .setValue(AnyValue.newBuilder()
                                .setStringValue("java")
                                .build()
                        )
                        .build()
                )
                .addAttributes(1, KeyValue.newBuilder()
                        .setKey("telemetry.sdk.version")
                        .setValue(AnyValue.newBuilder()
                                .setDoubleValue(0.8)
                                .build()
                        )
                        .build()
                )
                .addAttributes(2, KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder()
                                .setStringValue("analytics-service")
                                .build()
                        )
                        .build()
                )
                .build();


        Span span = Span.newBuilder()
                .setTraceId(ByteString.copyFrom(traceIdBuffer.array()))
                .setSpanId(ByteString.copyFrom(spanIdBuffer.array()))
                .setName(name)
                .setStartTimeUnixNano(time)
                .setEndTimeUnixNano(time + 1L)
                .addAttributes(0, KeyValue.newBuilder()
                        .setKey("net.peer.port")
                        .setValue(AnyValue.newBuilder()
                                .setIntValue(41164)
                                .build()
                        )
                        .build()
                )
                .addAttributes(1, KeyValue.newBuilder()
                        .setKey("servlet.path")
                        .setValue(AnyValue.newBuilder()
                                .setStringValue("/logs")
                                .build()
                        )
                        .build()
                )
                .build();


        InstrumentationLibrary instrumentationLibrary = InstrumentationLibrary.newBuilder()
                .setName(instrumentationName)
                .setVersion(instrumentationVersion)
                .build();


        RawTrace rawTrace = RawTrace.buildFromProto(resource, span, instrumentationLibrary);
        assertThat(rawTrace).isNotNull();
        assertThat(rawTrace.getTraceId().equals(Hex.encodeHexString(span.getTraceId().toByteArray())));
        assertThat(rawTrace.getSpanId().equals(Hex.encodeHexString(span.getSpanId().toByteArray())));
        assertThat(rawTrace.getName().equals("LoggingController.save"));
        assertThat(rawTrace.getStartTime().equals(RawTrace.convertStringNanosToISO8601(String.valueOf(span.getStartTimeUnixNano()))));
        assertThat(rawTrace.getEndTime().equals(rawTrace.convertStringNanosToISO8601(String.valueOf(span.getEndTimeUnixNano()))));
        assertThat(rawTrace.getServiceName().equals("analytics-service"));
        assertThat(rawTrace.getResourceAttributes().get("telemetry.sdk.version"));
        assertThat(rawTrace.getSpanAttributes().get("servlet.path"));
        assertThat(rawTrace.getInstrumentationName().equals(instrumentationName));
        assertThat(rawTrace.getInstrumentationVersion().equals(instrumentationVersion));
    }
}

