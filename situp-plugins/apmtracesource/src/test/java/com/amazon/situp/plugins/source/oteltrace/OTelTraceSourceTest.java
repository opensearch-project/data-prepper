package com.amazon.situp.plugins.source.oteltrace;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.source.oteltracesource.OTelTraceSource;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OTelTraceSourceTest {

    private static final ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                            .addSpans(Span.newBuilder().setTraceState("SUCCESS").build())).build()).build();
    private static OTelTraceSource SOURCE;
    private static final ExportTraceServiceRequest FAILURE_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                            .addSpans(Span.newBuilder().setTraceState("FAILURE").build())).build()).build();
    private static TraceServiceGrpc.TraceServiceBlockingStub CLIENT;
    private static final BlockingBuffer<Record<ExportTraceServiceRequest>> BUFFER = getBuffer();

    private static String getUri() {
        return "gproto+http://127.0.0.1:" + SOURCE.getoTelTraceSourceConfig().getPort() + '/';
    }

    private static BlockingBuffer<Record<ExportTraceServiceRequest>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }

    @BeforeEach
    public void beforeEach() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("request_timeout", 1);
        SOURCE = new OTelTraceSource(new PluginSetting("otel_trace_source", integerHashMap));
        SOURCE.start(BUFFER);
        CLIENT = Clients.newClient(getUri(), TraceServiceGrpc.TraceServiceBlockingStub.class);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    @Test
    void testBufferFull() {
        CLIENT.export(SUCCESS_REQUEST);
        try {
            CLIENT.export(ExportTraceServiceRequest.newBuilder().build());
        } catch (RuntimeException ex) {
            System.out.println("Printing the exception:" + ex);
        }
        validateBuffer();
    }

    private void validateBuffer() {
        List<Record<ExportTraceServiceRequest>> drainedBuffer = (List<Record<ExportTraceServiceRequest>>) BUFFER.read(100000);
        assertThat(drainedBuffer.size()).isEqualTo(1);
        assertThat(drainedBuffer.get(0).getData()).isEqualTo(SUCCESS_REQUEST);
    }

    @Test
    void testHttpFullJson() throws InvalidProtocolBufferException {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status().code()).isEqualTo(200);
                });
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status().code()).isEqualTo(503);
                    validateBuffer();
                });

    }

    @Test
    void testHttpFullBytes() {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(SUCCESS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status().code()).isEqualTo(200);
                });
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(FAILURE_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status().code()).isEqualTo(503);
                    validateBuffer();
                });
    }
}
