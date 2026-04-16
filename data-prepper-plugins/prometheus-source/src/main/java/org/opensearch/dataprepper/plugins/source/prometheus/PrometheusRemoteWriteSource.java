/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.http.BaseHttpSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.LoggerFactory;

/**
 * A Data Prepper source plugin that receives Prometheus Remote Write requests.
 * This plugin acts as a Remote Write receiver, accepting metrics pushed from Prometheus
 * servers or other Remote Write compatible clients.
 *
 * <p>Configuration example:</p>
 * <pre>
 * prometheus-source:
 *   port: 9090
 *   path: /api/v1/write
 *   ssl: true
 *   ssl_certificate_file: /path/to/cert.pem
 *   ssl_key_file: /path/to/key.pem
 * </pre>
 */
@Experimental
@DataPrepperPlugin(name = "prometheus", pluginType = Source.class, pluginConfigurationType = PrometheusRemoteWriteSourceConfig.class)
public class PrometheusRemoteWriteSource extends BaseHttpSource<Record<Event>> {

    private static final String SOURCE_NAME = "Prometheus Remote Write";

    private final PrometheusRemoteWriteSourceConfig sourceConfig;

    @DataPrepperPluginConstructor
    public PrometheusRemoteWriteSource(final PrometheusRemoteWriteSourceConfig sourceConfig,
                                        final PluginMetrics pluginMetrics,
                                        final PluginFactory pluginFactory,
                                        final PipelineDescription pipelineDescription) {
        super(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription, SOURCE_NAME,
                LoggerFactory.getLogger(PrometheusRemoteWriteSource.class));
        this.sourceConfig = sourceConfig;
    }

    @Override
    public BaseHttpService getHttpService(final int bufferWriteTimeoutInMillis, final Buffer<Record<Event>> buffer, final PluginMetrics pluginMetrics) {
        return new PrometheusRemoteWriteService(bufferWriteTimeoutInMillis, buffer, pluginMetrics,
                new RemoteWriteProtobufParser(sourceConfig));
    }
}
