package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.client.Clients;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;

@ExtendWith(MockitoExtension.class)
public class OTelTelemetrySource_RetryInfoTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21893/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock(lenient = true)
    private OTelTelemetrySourceConfig oTelTelemetrySourceConfig;

    @Mock
    private Buffer<Record<Object>> buffer;

    private OTelTelemetrySource SOURCE;

    @BeforeEach
    void beforeEach() throws Exception {
        lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();
        Mockito.lenient().doThrow(SizeOverflowException.class).when(buffer).writeAll(any(), anyInt());

        when(oTelTelemetrySourceConfig.getPort()).thenReturn(21893);
        when(oTelTelemetrySourceConfig.isSsl()).thenReturn(false);
        when(oTelTelemetrySourceConfig.getRequestTimeoutInMillis()).thenReturn(1000);
        when(oTelTelemetrySourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelTelemetrySourceConfig.getThreadCount()).thenReturn(5);
        when(oTelTelemetrySourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTelemetrySourceConfig.getRetryInfo()).thenReturn(TEST_RETRY_INFO);

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
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("otel_telemetry", "pipeline");
        PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        SOURCE = new OTelTelemetrySource(oTelTelemetrySourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @Test
    public void gRPC_failed_request_returns_minimal_delay_in_status() throws Exception {
        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportMetricsRequest()));

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

    private RetryInfo callService3TimesAndReturnRetryInfo() throws Exception {
        StatusRuntimeException e = null;
        for (int i = 0; i < 3; i++) {
            final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                    .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
            e = assertThrows(StatusRuntimeException.class, () -> client.export(createExportMetricsRequest()));
        }

        return extractRetryInfoFromStatusRuntimeException(e);
    }

    private ExportMetricsServiceRequest createExportMetricsRequest() {
        return ExportMetricsServiceRequest.newBuilder().build();
    }
}
