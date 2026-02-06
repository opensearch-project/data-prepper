/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createConfigBuilderWithBasicAuth;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createDefaultConfig;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_USERNAME;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;

import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

@ExtendWith(MockitoExtension.class)
class OTelLogsSourceGrpcServiceRequestsTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21892/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private GrpcServiceBuilder grpcServiceBuilder;

    @Mock
    private GrpcService grpcService;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock
    private ArmeriaHttpAuthenticationProvider httpAuthenticationProvider;

    @Mock
    private BlockingBuffer<Record<Object>> buffer;

    private PluginMetrics pluginMetrics;
    private OTelLogsSource SOURCE;

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(serverBuilder.port(anyInt(), (SessionProtocol) any())).thenReturn(serverBuilder);
        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        lenient().when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(httpAuthenticationProvider);

        setupLogsSource(createDefaultConfig());
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void setupLogsSource(OTelLogsSourceConfig config) {
        PluginSetting settings = new PluginSetting("OTelLogsGrpcService", null);
        settings.setPipelineName(TEST_PIPELINE_NAME);
        SOURCE = new OTelLogsSource(config, pluginMetrics, pluginFactory, settings);
    }

    @Test
    void logsExportRequest_isProcessed_resultIsWrittenToBuffer() throws Exception {
        SOURCE.start(buffer);

        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        final ExportLogsServiceResponse exportResponse = client.export(createExportLogsRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<Object>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<Object>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites, hasSize(1));
    }

    @ParameterizedTest
    @MethodSource("getBasicAuthTestData")
    void logsExportRequest_withBasicAuth_returnsAppropriateResponse(String givenUsername, String givenPassword, Status.Code expectedStatus, VerificationMode expectedBufferWrites) throws Exception {
        GrpcBasicAuthenticationProvider authProvider = new GrpcBasicAuthenticationProvider(new HttpBasicAuthenticationConfig(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD));
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        setupLogsSource(createConfigBuilderWithBasicAuth().build());
        final String encodeToString = Base64.getEncoder().encodeToString(String.format("%s:%s", givenUsername, givenPassword).getBytes(StandardCharsets.UTF_8));
        SOURCE.start(buffer);
        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + encodeToString)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        try {
            ExportLogsServiceResponse response = client.export(createExportLogsRequest());
            assertThat(response, notNullValue());
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode(), is(expectedStatus));
        }

        verify(buffer, expectedBufferWrites).writeAll(any(), anyInt());
    }

    private static Stream<Arguments> getBasicAuthTestData() {
        return Stream.of(
                Arguments.of(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD, Status.Code.OK, times(1)),
                Arguments.of(BASIC_AUTH_USERNAME, "wrong password", Status.Code.UNAUTHENTICATED, times(0))
        );
    }

    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    void logsExportRequest_bufferThrowsException_returnsAppropriateResponse(
            final Class<Exception> bufferExceptionClass,
            final Status.Code expectedStatusCode) throws Exception {
        doThrow(bufferExceptionClass)
                .when(buffer)
                .writeAll(anyCollection(), anyInt());
        SOURCE.start(buffer);
        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT).build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportLogsRequest()));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(expectedStatusCode));
    }

    static class BufferExceptionToStatusArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(TimeoutException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(SizeOverflowException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(Exception.class, Status.Code.INTERNAL),
                    arguments(RuntimeException.class, Status.Code.INTERNAL)
            );
        }
    }

    private ExportLogsServiceRequest createExportLogsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .addLogRecords(LogRecord.newBuilder().setSeverityNumberValue(1))
                        .build())
                .setResource(resource)
                .build();

        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
    }
}
