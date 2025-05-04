package org.opensearch.dataprepper.plugins.source.otlp;

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
import java.util.UUID;

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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.client.Clients;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

@ExtendWith(MockitoExtension.class)
public class OTLPSource_RetryInfoTest {
  private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21893/";
  private static final String TEST_PIPELINE_NAME = "test_pipeline";
  private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100),
      Duration.ofMillis(2000));

  @Mock
  private PluginFactory pluginFactory;

  @Mock
  private GrpcBasicAuthenticationProvider authenticationProvider;

  @Mock(lenient = true)
  private OTLPSourceConfig otlpSourceConfig;

  @Mock
  private Buffer<Record<Object>> buffer;

  private OTLPSource SOURCE;

  @BeforeEach
  void beforeEach() throws Exception {
    lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();
    Mockito.lenient().doThrow(SizeOverflowException.class).when(buffer).writeAll(any(), anyInt());

    when(otlpSourceConfig.getPort()).thenReturn(21893);
    when(otlpSourceConfig.isSsl()).thenReturn(false);
    when(otlpSourceConfig.getRequestTimeoutInMillis()).thenReturn(1000);
    when(otlpSourceConfig.getMaxConnectionCount()).thenReturn(10);
    when(otlpSourceConfig.getThreadCount()).thenReturn(5);
    when(otlpSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
    when(otlpSourceConfig.getRetryInfo()).thenReturn(TEST_RETRY_INFO);

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
    PluginMetrics pluginMetrics = PluginMetrics.fromNames("otlp-source", "pipeline");
    PipelineDescription pipelineDescription = mock(PipelineDescription.class);
    lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

    SOURCE = new OTLPSource(otlpSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
  }

  @Test
  public void metrics_failed_request_returns_minimal_delay_in_status() throws Exception {
    final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
        .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> client.export(createExportMetricsRequest()));

    RetryInfo retryInfo = extractRetryInfoFromStatusRuntimeException(statusRuntimeException);
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(100L));
  }

  @Test
  public void logs_failed_request_returns_minimal_delay_in_status() throws Exception {
    final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
        .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> client.export(createExportLogsRequest()));

    RetryInfo retryInfo = extractRetryInfoFromStatusRuntimeException(statusRuntimeException);
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(100L));
  }

  @Test
  public void traces_failed_request_returns_minimal_delay_in_status() throws Exception {
    final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
        .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> client.export(createExportTraceRequest()));

    RetryInfo retryInfo = extractRetryInfoFromStatusRuntimeException(statusRuntimeException);
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(100L));
  }

  @Test
  public void metrics_failed_request_returns_extended_delay_in_status() throws Exception {
    RetryInfo retryInfo = callMetricService3TimesAndReturnRetryInfo();
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(200L));
  }

  @Test
  public void logs_failed_request_returns_extended_delay_in_status() throws Exception {
    RetryInfo retryInfo = callLogsService3TimesAndReturnRetryInfo();
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(200L));
  }

  @Test
  public void traces_failed_request_returns_extended_delay_in_status() throws Exception {
    RetryInfo retryInfo = callTraceService3TimesAndReturnRetryInfo();
    assertThat(Duration.ofNanos(retryInfo.getRetryDelay().getNanos()).toMillis(), equalTo(200L));
  }

  private RetryInfo extractRetryInfoFromStatusRuntimeException(StatusRuntimeException e)
      throws InvalidProtocolBufferException {
    com.google.rpc.Status status = com.google.rpc.Status
        .parseFrom(e.getTrailers().get(Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER)));
    return RetryInfo.parseFrom(status.getDetails(0).getValue());
  }

  private RetryInfo callMetricService3TimesAndReturnRetryInfo() throws Exception {
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

  private RetryInfo callLogsService3TimesAndReturnRetryInfo() throws Exception {
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
            .setValue(AnyValue.newBuilder().setStringValue("service").build()))
        .build();

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

  private RetryInfo callTraceService3TimesAndReturnRetryInfo() throws Exception {
    StatusRuntimeException e = null;
    for (int i = 0; i < 3; i++) {
      final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
          .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
      e = assertThrows(StatusRuntimeException.class, () -> client.export(createExportTraceRequest()));
    }

    return extractRetryInfoFromStatusRuntimeException(e);
  }

  private ExportTraceServiceRequest createExportTraceRequest() {
    final Span testSpan = Span.newBuilder()
        .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
        .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
        .setName(UUID.randomUUID().toString())
        .setKind(Span.SpanKind.SPAN_KIND_SERVER)
        .setStartTimeUnixNano(100)
        .setEndTimeUnixNano(101)
        .setTraceState("SUCCESS").build();

    ScopeSpans scopeSpan = ScopeSpans.newBuilder().addSpans(testSpan).build();
    return ExportTraceServiceRequest.newBuilder()
        .addResourceSpans(ResourceSpans.newBuilder().addScopeSpans(scopeSpan)).build();
  }
}
