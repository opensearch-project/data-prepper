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
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "rss", pluginType = Source.class, pluginConfigurationType =  RSSSourceConfig.class)
public class RSSSource implements Source<Record<Document>> {

    private static final Logger LOG = LoggerFactory.getLogger(RSSSource.class);

    private final PluginMetrics pluginMetrics;

    private final RSSSourceConfig rssSourceConfig;

    private static int count = 0;

    @DataPrepperPluginConstructor
    public RSSSource(final PluginMetrics pluginMetrics, final RSSSourceConfig rssSourceConfig) throws InterruptedException {
        this.pluginMetrics = pluginMetrics;
        this.rssSourceConfig = rssSourceConfig;
    }

    @Override
    public void start(Buffer buffer) {
        // TODO: Add to buffer
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
    }

    @Override
    public void stop() {
        // TODO: Stop the service
    }

//    public void run() throws InterruptedException {
//        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
//
//        Runnable task = () -> {
//            readRssRead();
//            count++;
//            System.out.println("Running task to extract items from RSS feed");
//        };
//
//        ScheduledFuture<?> scheduledFuture = executorService.scheduleAtFixedRate(task, 0, 5, TimeUnit.MINUTES);
//
//        while (true) {
//            System.out.println("Count: "+ count);
//            Thread.sleep(1000);
//            if (count == 20) {
//                System.out.println(" Count is 20 time to cancel scheduled future");
//                scheduledFuture.cancel(true);
//                executorService.shutdown();
//                break;
//            }
//        }
//    }
    void extractItemsFromRssFeed(final RSSSourceConfig rssSourceConfig) throws InterruptedException {

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        final String url = rssSourceConfig.getUrl();
        if(url.isEmpty()) {
            throw new IllegalArgumentException("No path specified for the RSS Feed URL");
        }
        final RssReader reader = new RssReader();

        Runnable task = () -> {
            final Stream<Item> rssFeed;
            try {
                rssFeed = reader.read(url);
            } catch (IOException e) {
                throw new RuntimeException("IO Exception when reading RSS Feed URL", e);
            }
            final List<Item> items = rssFeed.collect(Collectors.toList());
            items.forEach(this::buildEventDocument);
            count++;
            System.out.println("Running task to extract items from RSS feed");
        };

        ScheduledFuture<?> scheduledFuture = executorService.scheduleAtFixedRate(task, 0, 5, TimeUnit.MINUTES);

        while (true) {
            System.out.println("Count: "+ count);
            Thread.sleep(1000);
            if (count == 1) {
                System.out.println(" Count is 20 time to cancel scheduled future");
                scheduledFuture.cancel(true);
                executorService.shutdown();
                break;
            }
        }

    }

    private Record<Document> buildEventDocument(final Item item) {
        final JacksonDocument document = JacksonDocument.builder()
                .withData(item)
                .getThis()
                .build();
        return new Record<>(document);

    }
}
