/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_GRPC_PATH;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_HTTP_PATH;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.client.Clients;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

@ExtendWith(MockitoExtension.class)
class OtelLogsSource_RetryInfoTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21892/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock(lenient = true)
    private OTelLogsSourceConfig oTelLogsSourceConfig;

    @Mock
    private Buffer<Record<Object>> buffer;

    private OTelLogsSource SOURCE;

    @BeforeEach
    void beforeEach() throws Exception {
        lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();
        Mockito.lenient().doThrow(SizeOverflowException.class).when(buffer).writeAll(any(), anyInt());

        when(oTelLogsSourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelLogsSourceConfig.isSsl()).thenReturn(false);
        when(oTelLogsSourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelLogsSourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelLogsSourceConfig.getThreadCount()).thenReturn(5);
        when(oTelLogsSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelLogsSourceConfig.getRetryInfo()).thenReturn(TEST_RETRY_INFO);
        when(oTelLogsSourceConfig.getHttpPath()).thenReturn(CONFIG_HTTP_PATH);
        when(oTelLogsSourceConfig.getPath()).thenReturn(CONFIG_GRPC_PATH);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        configureObjectUnderTest();
        SOURCE.start(buffer);
    }

    @AfterEach
    void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");
        PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        SOURCE = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertInstanceOf(OTelLogsDecoder.class, SOURCE.getDecoder());
    }

    @Test
    public void gRPC_failed_request_returns_minimal_delay_in_status() throws Exception {
        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportLogsRequest()));

        RetryInfo retryInfo = extractRetryInfoFromStatusRuntimeException(statusRuntimeException);
        assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(100L));
    }

    @Test
    public void gRPC_failed_request_returns_extended_delay_in_status() throws Exception {
        RetryInfo retryInfo = callService3TimesAndReturnRetryInfo();

        assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(200L));
    }

    private RetryInfo extractRetryInfoFromStatusRuntimeException(StatusRuntimeException e) throws InvalidProtocolBufferException {
        com.google.rpc.Status status = com.google.rpc.Status.parseFrom(e.getTrailers().get(Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER)));
        return RetryInfo.parseFrom(status.getDetails(0).getValue());
    }

    /**
     * The back off is calculated with the second call, and returned with the third
     */
    private RetryInfo callService3TimesAndReturnRetryInfo() throws Exception {
        StatusRuntimeException e = null;
        for (int i = 0; i < 3; i++) {
            final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                    .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
            e = assertThrows(StatusRuntimeException.class, () -> client.export(createExportLogsRequest()));
        }

        return extractRetryInfoFromStatusRuntimeException(e);
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
