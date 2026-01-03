/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otlp;

import com.linecorp.armeria.server.Server;

import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsGrpcService;
import org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceGrpcService;
import org.opensearch.dataprepper.plugins.source.otlp.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.CreateServer.GRPCServiceConfig;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "otlp", pluginType = Source.class, pluginConfigurationType = OTLPSourceConfig.class)
public class OTLPSource implements Source<Record<Object>> {
  private static final Logger LOG = LoggerFactory.getLogger(OTLPSource.class);
  static final String SERVER_CONNECTIONS = "serverConnections";

  private final OTLPSourceConfig otlpSourceConfig;
  private final String pipelineName;
  private final PluginMetrics pluginMetrics;
  private final GrpcAuthenticationProvider authenticationProvider;
  private final CertificateProviderFactory certificateProviderFactory;
  private Server server;

  @DataPrepperPluginConstructor
  public OTLPSource(final OTLPSourceConfig otlpSourceConfig,
      final PluginMetrics pluginMetrics,
      final PluginFactory pluginFactory,
      final PipelineDescription pipelineDescription) {
    this(otlpSourceConfig, pluginMetrics, pluginFactory,
        new CertificateProviderFactory(otlpSourceConfig), pipelineDescription);
  }

  // accessible only in the same package for unit test
  OTLPSource(final OTLPSourceConfig otlpSourceConfig,
      final PluginMetrics pluginMetrics,
      final PluginFactory pluginFactory,
      final CertificateProviderFactory certificateProviderFactory,
      final PipelineDescription pipelineDescription) {
    otlpSourceConfig.validateAndInitializeCertAndKeyFileInS3();
    this.otlpSourceConfig = otlpSourceConfig;
    this.pluginMetrics = pluginMetrics;
    this.certificateProviderFactory = certificateProviderFactory;
    this.pipelineName = pipelineDescription.getPipelineName();
    this.authenticationProvider = createAuthenticationProvider(pluginFactory);
  }

  @Override
  public void start(Buffer<Record<Object>> buffer) {
    if (buffer == null) {
      throw new IllegalStateException("Buffer provided is null");
    }

    if (server == null) {
      @SuppressWarnings("unchecked")
      Buffer<Record<? extends Metric>> metricBuffer = (Buffer<Record<? extends Metric>>) (Object) buffer;

      final OTelLogsGrpcService oTelLogsGrpcService = new OTelLogsGrpcService(
          (int) (otlpSourceConfig.getRequestTimeoutInMillis() * 0.8),
          otlpSourceConfig.getLogsOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
          buffer, pluginMetrics, "otlp.logs");

      final OTelMetricsGrpcService oTelMetricsGrpcService = new OTelMetricsGrpcService(
          (int) (otlpSourceConfig.getRequestTimeoutInMillis() * 0.8),
          otlpSourceConfig.getMetricsOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
          metricBuffer, null, pluginMetrics, "otlp.metrics");

      final OTelTraceGrpcService oTelTraceGrpcService = new OTelTraceGrpcService(
          (int) (otlpSourceConfig.getRequestTimeoutInMillis() * 0.8),
          otlpSourceConfig.getTracesOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
          buffer, pluginMetrics, "otlp.traces");

      ServerConfiguration serverConfiguration = ConvertConfiguration.convertConfiguration(otlpSourceConfig);
      CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, "otlp", pipelineName);
      CertificateProvider certificateProvider = null;
      if (otlpSourceConfig.isSsl() || otlpSourceConfig.useAcmCertForSSL()) {
        certificateProvider = certificateProviderFactory.getCertificateProvider();
      }

      List<GRPCServiceConfig<?, ?>> serviceConfigs = new ArrayList<>();
      serviceConfigs.add(new GRPCServiceConfig<>(oTelLogsGrpcService, otlpSourceConfig.getLogsPath(),
          LogsServiceGrpc.getExportMethod()));
      serviceConfigs.add(new GRPCServiceConfig<>(oTelMetricsGrpcService, otlpSourceConfig.getMetricsPath(),
          MetricsServiceGrpc.getExportMethod()));
      serviceConfigs.add(new GRPCServiceConfig<>(oTelTraceGrpcService, otlpSourceConfig.getTracesPath(),
          TraceServiceGrpc.getExportMethod()));

      server = createServer.createGRPCServer(authenticationProvider, serviceConfigs, certificateProvider);

      pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
    }

    try {
      server.start().get();
    } catch (ExecutionException ex) {
      if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
        throw (RuntimeException) ex.getCause();
      } else {
        throw new RuntimeException(ex);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
    LOG.info("Started otlp source on port {}.", otlpSourceConfig.getPort());
  }

  @Override
  public void stop() {
    if (server != null) {
      try {
        server.stop().get();
      } catch (ExecutionException ex) {
        if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
          throw (RuntimeException) ex.getCause();
        } else {
          throw new RuntimeException(ex);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
    LOG.info("Stopped otlp source.");
  }

  private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory) {
    final PluginModel authenticationConfiguration = otlpSourceConfig.getAuthentication();

    if (authenticationConfiguration == null || authenticationConfiguration.getPluginName()
        .equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
      LOG.warn("Creating otlp source without authentication. This is not secure.");
      LOG.warn(
          "In order to set up Http Basic authentication for otlp source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otlp-source#authentication-configurations");
    }

    final PluginSetting authenticationPluginSetting;
    if (authenticationConfiguration != null) {
      authenticationPluginSetting = new PluginSetting(authenticationConfiguration.getPluginName(),
          authenticationConfiguration.getPluginSettings());
    } else {
      authenticationPluginSetting = new PluginSetting(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
          Collections.emptyMap());
    }
    authenticationPluginSetting.setPipelineName(pipelineName);
    return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, authenticationPluginSetting);
  }
}
