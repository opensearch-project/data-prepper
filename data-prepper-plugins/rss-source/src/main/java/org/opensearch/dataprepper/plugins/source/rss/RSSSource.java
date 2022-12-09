/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.document.JacksonDocument;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private void extractItems(final RSSSourceConfig rssSourceConfig) throws IOException {
        final String url = rssSourceConfig.getUrl();
        RssReader reader = new RssReader();
        Stream<Item> rssFeed = reader.read(url);
        List<Item> items = rssFeed.collect(Collectors.toList());


    }

    private Record<Document> buildEventDocument(Event event) {
        final JacksonDocument document = JacksonDocument.builder()
                .withData(event)
                .getThis()
                .build();

        return new Record<>(document);
    }
}
