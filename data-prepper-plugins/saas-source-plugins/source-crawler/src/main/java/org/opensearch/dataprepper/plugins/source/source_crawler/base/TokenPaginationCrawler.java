package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
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
public class TokenPaginationCrawler implements Crawler<PaginationCrawlerWorkerProgressState> {
    private static final Logger log = LoggerFactory.getLogger(TokenPaginationCrawler.class);
    private static final int batchSize = 50;
    private static final String PAGINATION_WORKER_PARTITIONS_CREATED = "paginationWorkerPartitionsCreated";
    private static final String INVALID_PAGINATION_ITEMS = "invalidPaginationItems";
    private static final String WORKER_PARTITION_WAIT_TIME = "WorkerPartitionWaitTime";
    private static final String WORKER_PARTITION_PROCESS_LATENCY = "WorkerPartitionProcessLatency";
    private final Timer crawlingTimer;
    private final Timer partitionWaitTimeTimer;
    private final Timer partitionProcessLatencyTimer;
    private final CrawlerClient client;
    private final Counter parititionsCreatedCounter;
    private final Counter invalidPaginationItemsCounter;

    public TokenPaginationCrawler(CrawlerClient client,
                                  PluginMetrics pluginMetrics) {
        this.client = client;
        this.crawlingTimer = pluginMetrics.timer("crawlingTime");
        this.parititionsCreatedCounter = pluginMetrics.counter(PAGINATION_WORKER_PARTITIONS_CREATED);
        this.partitionWaitTimeTimer = pluginMetrics.timer(WORKER_PARTITION_WAIT_TIME);
        this.partitionProcessLatencyTimer = pluginMetrics.timer(WORKER_PARTITION_PROCESS_LATENCY);
        this.invalidPaginationItemsCounter = pluginMetrics.counter(INVALID_PAGINATION_ITEMS);

    }

    @Override
    public Instant crawl(LeaderPartition leaderPartition,
                         EnhancedSourceCoordinator coordinator) {
        long startTime = System.currentTimeMillis();
        Instant lastLeaderSavedInstant = Instant.now();
        TokenPaginationCrawlerLeaderProgressState leaderProgressState = (TokenPaginationCrawlerLeaderProgressState) leaderPartition
                .getProgressState().get();
        String lastToken = leaderProgressState.getLastToken();
        Iterator<ItemInfo> itemInfoIterator = ((TokenCrawlerClient) client).listItems(lastToken);
        String latestToken = lastToken;
        log.info("Starting to crawl the source with last item ID: {}", lastToken);
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < batchSize && itemInfoIterator.hasNext(); i++) {
                final ItemInfo nextItem = itemInfoIterator.next();
                if (nextItem == null) {
                    //we don't expect null items, but just in case, we'll skip them
                    log.warn("Unexpected encounter of a null item while processing batch with last item ID " + lastToken);
                    invalidPaginationItemsCounter.increment();
                    continue;
                }
                itemInfoList.add(nextItem);
                if (nextItem.getItemId() != null) {
                    latestToken = nextItem.getItemId();
                }
            }
            if (!itemInfoList.isEmpty()) {
                createPartition(itemInfoList, coordinator);
            }
            // Check point leader progress state at every minute interval.
            Instant currentTimeInstance = Instant.now();
            if (Duration.between(lastLeaderSavedInstant, currentTimeInstance).toMinutes() >= 1) {
                // intermediate updates to master partition state
                updateLeaderProgressState(leaderPartition, latestToken, coordinator);
                lastLeaderSavedInstant = currentTimeInstance;
            }
        } while (itemInfoIterator.hasNext());
        updateLeaderProgressState(leaderPartition, latestToken, coordinator);
        long crawlTimeMillis = System.currentTimeMillis() - startTime;
        log.debug("Crawling completed in {} ms", crawlTimeMillis);
        crawlingTimer.record(crawlTimeMillis, TimeUnit.MILLISECONDS);
        return Instant.now(); // Return current time as required by Crawler interface
    }

    public void executePartition(PaginationCrawlerWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {
        partitionWaitTimeTimer.record(Duration.between(state.getExportStartTime(), Instant.now()));
        partitionProcessLatencyTimer.record(() -> client.executePartition(state, buffer, acknowledgementSet));
    }

    private void updateLeaderProgressState(LeaderPartition leaderPartition, String updatedToken, EnhancedSourceCoordinator coordinator) {
        TokenPaginationCrawlerLeaderProgressState leaderProgressState = (TokenPaginationCrawlerLeaderProgressState) leaderPartition
                .getProgressState().get();
        String oldToken = leaderProgressState.getLastToken();
        leaderProgressState.setLastToken(updatedToken);
        leaderPartition.setLeaderProgressState(leaderProgressState);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
        log.info("Updated leader progress state: old lastToken={}, new lastToken={}", oldToken, updatedToken);
    }

    private void createPartition(List<ItemInfo> itemInfoList, EnhancedSourceCoordinator coordinator) {
        if (itemInfoList.isEmpty()) {
            return;
        }
        ItemInfo itemInfo = itemInfoList.get(0);
        String partitionKey = itemInfo.getPartitionKey();
        List<String> itemIds = itemInfoList.stream().map(ItemInfo::getId).collect(Collectors.toList());
        PaginationCrawlerWorkerProgressState state = new PaginationCrawlerWorkerProgressState();
        state.setKeyAttributes(itemInfo.getKeyAttributes());
        state.setItemIds(itemIds);
        state.setExportStartTime(Instant.now());
        state.setLoadedItems(itemInfoList.size());
        SaasSourcePartition sourcePartition = new SaasSourcePartition(state, partitionKey);
        coordinator.createPartition(sourcePartition);
        parititionsCreatedCounter.increment();
    }
}
