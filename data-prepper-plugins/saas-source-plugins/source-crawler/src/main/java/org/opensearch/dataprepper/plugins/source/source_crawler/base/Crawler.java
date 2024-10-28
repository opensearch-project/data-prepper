package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Named
public class Crawler {
    private static final Logger log = LoggerFactory.getLogger(Crawler.class);
    private static final int maxItemsPerPage = 20;
    private final Timer crawlingTime;
    private final PluginMetrics pluginMetrics =
            PluginMetrics.fromNames("sourceCrawler", "crawler");

    private final CrawlerClient client;

    public Crawler(CrawlerClient client) {
        this.client = client;
        this.crawlingTime = pluginMetrics.timer("crawlingTime");
    }

    public Instant crawl(Instant lastPollTime,
                         EnhancedSourceCoordinator coordinator) {
        long startTime = System.currentTimeMillis();
        client.setLastPollTime(lastPollTime);
        Iterator<ItemInfo> itemInfoIterator = client.listItems();
        log.info("Starting to crawl the source with lastPollTime: {}", lastPollTime);
        Instant updatedPollTime = Instant.ofEpochMilli(0);
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < maxItemsPerPage && itemInfoIterator.hasNext(); i++) {
                ItemInfo nextItem = itemInfoIterator.next();
                if (nextItem == null) {
                    //we don't expect null items, but just in case, we'll skip them
                    log.info("Unexpected encounter of a null item.");
                    continue;
                }
                itemInfoList.add(nextItem);
                Instant lastModifiedTime = nextItem.getLastModifiedAt();
                updatedPollTime = updatedPollTime.isAfter(lastModifiedTime) ? updatedPollTime : lastModifiedTime;
            }
            createPartition(itemInfoList, coordinator);
        } while (itemInfoIterator.hasNext());
        long crawlTimeMillis = System.currentTimeMillis() - startTime;
        log.debug("Crawling completed in {} ms", crawlTimeMillis);
        crawlingTime.record(crawlTimeMillis, TimeUnit.MILLISECONDS);
        return updatedPollTime;
    }

    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig sourceConfig) {
        client.executePartition(state, buffer, sourceConfig);
    }

    private void createPartition(List<ItemInfo> itemInfoList, EnhancedSourceCoordinator coordinator) {
        if (itemInfoList.isEmpty()) {
            return;
        }
        ItemInfo itemInfo = itemInfoList.get(0);
        String partitionKey = itemInfo.getPartitionKey();
        List<String> itemIds = itemInfoList.stream().map(ItemInfo::getId).collect(Collectors.toList());
        SaasWorkerProgressState state = new SaasWorkerProgressState();
        state.setKeyAttributes(itemInfo.getKeyAttributes());
        state.setItemIds(itemIds);
        state.setExportStartTime(Instant.now());
        state.setLoadedItems(itemInfoList.size());
        SaasSourcePartition sourcePartition = new SaasSourcePartition(state, partitionKey);
        coordinator.createPartition(sourcePartition);
    }

}
