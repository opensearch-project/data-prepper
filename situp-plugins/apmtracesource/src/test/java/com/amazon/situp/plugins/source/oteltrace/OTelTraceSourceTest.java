package com.amazon.situp.plugins.source.oteltrace;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.source.oteltracesource.OTelTraceSource;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.*;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class OTelTraceSourceTest {

    private OTelTraceSource SOURCE;

    private String getUri() {
        return "gproto+http://127.0.0.1:" + SOURCE.getoTelTraceSourceConfig().getPort() + '/';
    }

    @BeforeEach
    private void beforeEach() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("request_timeout", 1);
        SOURCE = new OTelTraceSource(new PluginSetting("otel_trace_source", integerHashMap));
        SOURCE.start(getBuffer());
    }

    @AfterEach
    private void afterEach() {
        SOURCE.stop();
    }

    private BlockingBuffer<Record<ExportTraceServiceRequest>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }


    void addOneRequest() {
        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.newClient(getUri(), TraceServiceGrpc.TraceServiceBlockingStub.class);
        client.export(ExportTraceServiceRequest.newBuilder().build());
    }

    @Test
    void testSuccess(){
        addOneRequest();
    }

    @Test
    void testBufferFull() {
        addOneRequest();
        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.newClient(getUri(), TraceServiceGrpc.TraceServiceBlockingStub.class);
        try {
            client.export(ExportTraceServiceRequest.newBuilder().build());
            fail("Buffer should be full");
        } catch (RuntimeException ex) {
            assertThat(ex instanceof StatusRuntimeException).isTrue();
        }
    }

    @Test
    void testHttpFullJson() throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
        addOneRequest();
        final AggregatedHttpResponse res2 = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("0.0.0.0:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(ExportTraceServiceRequest.newBuilder().build()).getBytes())).aggregate().get();
        assertThat(res2.status().equals(HttpStatus.TOO_MANY_REQUESTS)).isTrue();
    }

    @Test
    void testHttpFullBytes() throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
        addOneRequest();
        final AggregatedHttpResponse res2 = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("0.0.0.0:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(ExportTraceServiceRequest.newBuilder().build().toByteArray())).aggregate().get();
        assertThat(res2.status().equals(HttpStatus.TOO_MANY_REQUESTS)).isTrue();
    }
}
