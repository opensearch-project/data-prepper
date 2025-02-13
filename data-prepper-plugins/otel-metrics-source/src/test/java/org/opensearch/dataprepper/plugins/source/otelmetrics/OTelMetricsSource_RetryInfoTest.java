/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;

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
import org.opensearch.dataprepper.model.metric.Metric;
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
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

@ExtendWith(MockitoExtension.class)
class OTelMetricsSource_RetryInfoTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21891/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock(lenient = true)
    private OTelMetricsSourceConfig oTelMetricsSourceConfig;

    @Mock
    private Buffer<Record<? extends Metric>> buffer;

    private OTelMetricsSource SOURCE;

    @BeforeEach
    void beforeEach() throws Exception {
        lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();
        Mockito.lenient().doThrow(SizeOverflowException.class).when(buffer).writeAll(any(), anyInt());

        when(oTelMetricsSourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelMetricsSourceConfig.isSsl()).thenReturn(false);
        when(oTelMetricsSourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelMetricsSourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelMetricsSourceConfig.getThreadCount()).thenReturn(5);
        when(oTelMetricsSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelMetricsSourceConfig.getRetryInfo()).thenReturn(TEST_RETRY_INFO);

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
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("otel_trace", "pipeline");

        PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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

    /**
     * The back off is calculated with the second call, and returned with the third
     */
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
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");
        ScopeMetrics scopeMetric = ScopeMetrics.newBuilder().addMetrics(metric).build();

        final ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetric)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics).build();
    }
}
