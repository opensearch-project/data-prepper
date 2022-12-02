/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.source.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperPlugin(name = "rss", pluginType = Source.class, pluginConfigurationType =  RSSSourceConfig.class)
public class RSSSource implements Source {

    private static final Logger LOG = LoggerFactory.getLogger(RSSSource.class);

    private final PluginMetrics pluginMetrics;

    private final RSSSourceConfig rssSourceConfig;

    @DataPrepperPluginConstructor
    public RSSSource(PluginMetrics pluginMetrics, final RSSSourceConfig rssSourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.rssSourceConfig = rssSourceConfig;
    }



    @Override
    public void start(Buffer buffer) {
        // TODO: Add to buffer
    }

    @Override
    public void stop() {
        // TODO: Stop the service
    }
}
