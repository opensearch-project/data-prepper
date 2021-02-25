package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.plugins.health.HealthGrpcService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@DataPrepperPlugin(name = "otel_trace_source", type = PluginType.SOURCE)
public class OTelTraceSource implements Source<Record<ExportTraceServiceRequest>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);
    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private Server server;
    private final PluginMetrics pluginMetrics;

    public OTelTraceSource(final PluginSetting pluginSetting) {
        oTelTraceSourceConfig = OTelTraceSourceConfig.buildConfig(pluginSetting);
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
    }

    @Override
    public void start(Buffer<Record<ExportTraceServiceRequest>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {
            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .addService(new OTelTraceGrpcService(
                            oTelTraceSourceConfig.getRequestTimeoutInMillis(),
                            buffer,
                            pluginMetrics
                    ))
                    .addService(new HealthGrpcService())
                    .useClientTimeoutHeader(false);

            if (oTelTraceSourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelTraceSourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            final ServerBuilder sb = Server.builder();
            sb.service(grpcServiceBuilder.build());
            sb.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());

            if (oTelTraceSourceConfig.isSsl()) {
                LOG.info("SSL/TLS is enabled");
                sb.https(oTelTraceSourceConfig.getPort()).tls(new File(oTelTraceSourceConfig.getSslKeyCertChainFile()),
                        new File(oTelTraceSourceConfig.getSslKeyFile()));
            } else {
                sb.http(oTelTraceSourceConfig.getPort());
            }

            sb.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
            sb.blockingTaskExecutor(
                    Executors.newScheduledThreadPool(oTelTraceSourceConfig.getThreadCount()),
                    true);

            server = sb.build();
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
        LOG.info("Started otel_trace_source...");
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
        LOG.info("Stopped otel_trace_source.");
    }

    public OTelTraceSourceConfig getoTelTraceSourceConfig() {
        return oTelTraceSourceConfig;
    }
}
