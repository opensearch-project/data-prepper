package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.BlockingBuffer;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.SSL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OTelTraceSourceTest {

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;
    PluginSetting pluginSetting;
    PluginSetting testPluginSetting;

    private BlockingBuffer<Record<ExportTraceServiceRequest>> buffer;

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
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        final HashMap<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 1);
        settingsMap.put(SSL, false);
        pluginSetting = new PluginSetting("otel_trace", settingsMap);
        pluginSetting.setPipelineName("pipeline");
        SOURCE = new OTelTraceSource(pluginSetting);

        buffer = getBuffer();
        SOURCE.start(buffer);

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
        Map.Entry<Collection<Record<ExportTraceServiceRequest>>, CheckpointState> drainedBufferResult = buffer.read(100000);
        List<Record<ExportTraceServiceRequest>> drainedBuffer = (List<Record<ExportTraceServiceRequest>>) drainedBufferResult.getKey();
        CheckpointState checkpointState = drainedBufferResult.getValue();
        assertThat(drainedBuffer.size()).isEqualTo(1);
        assertThat(drainedBuffer.get(0).getData()).isEqualTo(SUCCESS_REQUEST);
        assertEquals(1, checkpointState.getNumRecordsToBeChecked());
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
                    assertThat(i.status().code()).isEqualTo(415);
                }).join();
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
                    assertThat(i.status().code()).isEqualTo(415);
                    //validateBuffer();
                }).join();

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
                    assertThat(i.status().code()).isEqualTo(415);
                }).join();
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
                    assertThat(i.status().code()).isEqualTo(415);
                    //validateBuffer();
                }).join();
    }

    @Test
    public void testDoubleStart() {
        Assertions.assertThrows(IllegalStateException.class, () -> SOURCE.start(buffer));
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        final OTelTraceSource source = new OTelTraceSource(testPluginSetting);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        final OTelTraceSource source = new OTelTraceSource(testPluginSetting);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, source::stop);
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(pluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(() -> Server.builder()).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, source::stop);
            assertTrue(Thread.interrupted());
        }
    }
}
