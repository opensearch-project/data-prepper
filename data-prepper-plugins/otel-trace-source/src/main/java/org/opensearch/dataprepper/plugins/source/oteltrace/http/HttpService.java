package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServerBuilder;

public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);
    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginMetrics pluginMetrics;

    public HttpService(OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics) {
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
    }

    public void create(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        // todo tlongo what about tls?
        LOG.info("Creating http service");
        serverBuilder.annotatedService(new ArmeriaHttpService(buffer, pluginMetrics));
    }
}
