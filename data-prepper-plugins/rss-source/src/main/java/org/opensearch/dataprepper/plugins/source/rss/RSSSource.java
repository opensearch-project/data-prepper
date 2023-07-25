/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.RssReader;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@DataPrepperPlugin(name = "rss", pluginType = Source.class, pluginConfigurationType =  RSSSourceConfig.class)
public class RSSSource implements Source<Record<Document>> {

    private final PluginMetrics pluginMetrics;

    private final RSSSourceConfig rssSourceConfig;

    private RssReaderTask rssReaderTask;

    private final RssReader rssReader = new RssReader();

    private final ScheduledExecutorService scheduledExecutorService;


    @DataPrepperPluginConstructor
    public RSSSource(final PluginMetrics pluginMetrics, final RSSSourceConfig rssSourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.rssSourceConfig = rssSourceConfig;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void start(final Buffer<Record<Document>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        rssReaderTask = new RssReaderTask(rssReader, rssSourceConfig.getUrl(), buffer);
        scheduledExecutorService.scheduleAtFixedRate(rssReaderTask, 0,
                rssSourceConfig.getPollingFrequency().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

}
