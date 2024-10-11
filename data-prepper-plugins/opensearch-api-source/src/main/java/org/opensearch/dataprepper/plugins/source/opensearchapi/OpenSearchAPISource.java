/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.http.BaseHttpSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.LoggerFactory;

@DataPrepperPlugin(name = "opensearch_api", pluginType = Source.class, pluginConfigurationType = OpenSearchAPISourceConfig.class)
public class OpenSearchAPISource extends BaseHttpSource<Record<Event>> {
    private static final String SOURCE_NAME = "OpenSearch API";
    private static final String HTTP_HEALTH_CHECK_PATH = "/";

    @DataPrepperPluginConstructor
    public OpenSearchAPISource(final OpenSearchAPISourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                               final PipelineDescription pipelineDescription) {
        super(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription, SOURCE_NAME, LoggerFactory.getLogger(OpenSearchAPISource.class));
    }

    @Override
    public BaseHttpService getHttpService(final int bufferWriteTimeoutInMillis, final Buffer<Record<Event>> buffer, final PluginMetrics pluginMetrics) {
        return new OpenSearchAPIService(bufferWriteTimeoutInMillis, buffer, pluginMetrics);
    }

    @Override
    public String getHttpHealthCheckPath() {
        return HTTP_HEALTH_CHECK_PATH;
    }
}
