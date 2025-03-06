package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

@Named
public class Crawler {
    private static final Logger log = LoggerFactory.getLogger(Crawler.class);
    private final Timer crawlingTimer;

    private final CrawlerClient client;

    public Crawler(CrawlerClient client, PluginMetrics pluginMetrics) {
        this.client = client;
        this.crawlingTimer = pluginMetrics.timer("crawlingTime");
    }

    public Instant crawl(LeaderPartition leaderPartition,
                         EnhancedSourceCoordinator coordinator, int batchSize, Duration pollingTimezoneOffset) {
        long startTime = System.currentTimeMillis();
        Instant lastLeaderSavedInstant = Instant.now();
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        // Leader state is always saved in UTC since the timestamps in the api response are always in UTC
        Instant lastPollTime = leaderProgressState.getLastPollTime();
        // Adjust the time specific to the crawling source service setting before polling for changes
        Instant lastPollTimeWithOffsetAdjustment = lastPollTime.plusSeconds(pollingTimezoneOffset.toSeconds());
        client.setLastPollTime(lastPollTimeWithOffsetAdjustment);
        Iterator<ItemInfo> itemInfoIterator = client.listItems();
        Instant latestModifiedTime = lastPollTime;
        log.info("Starting to crawl the source with lastPollTime: {}", lastPollTimeWithOffsetAdjustment);
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < batchSize && itemInfoIterator.hasNext(); i++) {
                ItemInfo nextItem = itemInfoIterator.next();
                if (nextItem == null) {
                    //we don't expect null items, but just in case, we'll skip them
                    log.info("Unexpected encounter of a null item.");
                    continue;
                }
                itemInfoList.add(nextItem);
                if (nextItem.getLastModifiedAt().isAfter(latestModifiedTime)) {
                    latestModifiedTime = nextItem.getLastModifiedAt();
                }
            }
            createPartition(itemInfoList, coordinator);

            // Check point leader progress state at every minute interval.
            Instant currentTimeInstance = Instant.now();
            if (Duration.between(lastLeaderSavedInstant, currentTimeInstance).toMinutes() >= 1) {
                // intermediate updates to master partition state
                updateLeaderProgressState(leaderPartition, latestModifiedTime, coordinator);
                lastLeaderSavedInstant = currentTimeInstance;
            }

        } while (itemInfoIterator.hasNext());
        updateLeaderProgressState(leaderPartition, latestModifiedTime, coordinator);
        long crawlTimeMillis = System.currentTimeMillis() - startTime;
        log.debug("Crawling completed in {} ms", crawlTimeMillis);
        crawlingTimer.record(crawlTimeMillis, TimeUnit.MILLISECONDS);
        return latestModifiedTime;
    }

    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {
        client.executePartition(state, buffer, acknowledgementSet);
    }

    private void updateLeaderProgressState(LeaderPartition leaderPartition, Instant updatedPollTime, EnhancedSourceCoordinator coordinator) {
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setLastPollTime(updatedPollTime);
        leaderPartition.setLeaderProgressState(leaderProgressState);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
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
