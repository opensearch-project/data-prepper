package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.Setter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;
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

@Named
public class LeaderOnlyTokenCrawler implements Crawler {
    private static final Logger log = LoggerFactory.getLogger(LeaderOnlyTokenCrawler.class);
    private static final Duration BUFFER_WRITE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(1);
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(15);
    private static final int BATCH_SIZE = 50;

    private static final String METRIC_BATCHES_FAILED = "batchesFailed";
    private static final String METRIC_BUFFER_WRITE_TIME = "bufferWriteTime";
    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";

    private final LeaderOnlyTokenCrawlerClient client;
    private final Timer crawlingTimer;
    private final PluginMetrics pluginMetrics;
    @Setter
    private boolean acknowledgementsEnabled;
    @Setter
    private AcknowledgementSetManager acknowledgementSetManager;
    @Setter
    private Buffer<Record<Event>> buffer;
    private final Counter batchesFailedCounter;
    private final Counter acknowledgementSetSuccesses;
    private final Counter acknowledgementSetFailures;
    private final Timer bufferWriteTimer;

    private String lastToken;

    public LeaderOnlyTokenCrawler(
            LeaderOnlyTokenCrawlerClient client,
            PluginMetrics pluginMetrics) {
        this.client = client;
        this.pluginMetrics = pluginMetrics;
        this.crawlingTimer = pluginMetrics.timer("crawlingTime");
        this.batchesFailedCounter = pluginMetrics.counter(METRIC_BATCHES_FAILED);
        this.bufferWriteTimer = pluginMetrics.timer(METRIC_BUFFER_WRITE_TIME);
        this.acknowledgementSetSuccesses = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME);
        this.acknowledgementSetFailures = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME);
    }

    @Override
    public Instant crawl(LeaderPartition leaderPartition,
                         EnhancedSourceCoordinator coordinator) {
        long startTime = System.currentTimeMillis();
        Instant lastCheckpointTime = Instant.now();
        TokenPaginationCrawlerLeaderProgressState leaderProgressState =
                (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        lastToken = leaderProgressState.getLastToken();

        log.info("Starting leader-only crawl with token: {}", lastToken);

        Iterator<ItemInfo> itemIterator = client.listItems(lastToken);

        while (itemIterator.hasNext()) {
            List<ItemInfo> batch = collectBatch(itemIterator);
            if (batch.isEmpty()) {
                continue;
            }

            ItemInfo lastItem = batch.get(batch.size() - 1);
            lastToken = lastItem.getItemId();

            try {
                processBatch(batch, leaderPartition, coordinator);
            } catch (Exception e) {
                batchesFailedCounter.increment();
                log.error("Failed to process batch ending with token {}", lastToken, e);
                throw e;
            }

            // Periodic checkpoint if not using acknowledgments
            if (!acknowledgementsEnabled &&
                    Duration.between(lastCheckpointTime, Instant.now()).compareTo(CHECKPOINT_INTERVAL) >= 0) {
                updateLeaderProgressState(leaderPartition, lastToken, coordinator);
                lastCheckpointTime = Instant.now();
            }
        }

        // Final flush of any remaining items
        if (!acknowledgementsEnabled) {
            updateLeaderProgressState(leaderPartition, lastToken, coordinator);
        }

        long crawlTimeMillis = System.currentTimeMillis() - startTime;
        log.debug("Crawling completed in {} ms", crawlTimeMillis);
        crawlingTimer.record(crawlTimeMillis, TimeUnit.MILLISECONDS);
        return Instant.now();
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer buffer, AcknowledgementSet acknowledgementSet) {

    }

    private List<ItemInfo> collectBatch(Iterator<ItemInfo> iterator) {
        List<ItemInfo> batch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE && iterator.hasNext(); i++) {
            ItemInfo item = iterator.next();
            if (item != null) {
                batch.add(item);
            }
        }
        return batch;
    }

    private void processBatch(List<ItemInfo> batch,
                              LeaderPartition leaderPartition,
                              EnhancedSourceCoordinator coordinator) {
        if (acknowledgementsEnabled) {
            AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(
                    success -> {
                        if (success) {
                            // On success: update checkpoint
                            acknowledgementSetSuccesses.increment();
                            updateLeaderProgressState(leaderPartition, lastToken, coordinator);
                        } else {
                            // On failure: give up partition
                            acknowledgementSetFailures.increment();
                            log.error("Batch processing failed for token: {}", lastToken);
                            coordinator.giveUpPartition(leaderPartition);
                        }
                    },
                    BUFFER_WRITE_TIMEOUT
            );

            bufferWriteTimer.record(() -> {
                try {
                    client.writeBatchToBuffer(batch, buffer, acknowledgementSet);
                    acknowledgementSet.complete();
                } catch (Exception e) {
                    log.error("Failed to write batch to buffer", e);
                    acknowledgementSet.complete();  // This will trigger the failure callback
                    throw e;
                }
            });
        } else {
            // Without Acknowledgments:
            // Write directly and update checkpoint
            bufferWriteTimer.record(() -> {
                try {
                    client.writeBatchToBuffer(batch, buffer, null);
                    updateLeaderProgressState(leaderPartition, lastToken, coordinator);
                } catch (Exception e) {
                    log.error("Failed to write batch to buffer", e);
                    throw e;
                }
            });
        }
    }

    private void updateLeaderProgressState(LeaderPartition leaderPartition,
                                           String updatedToken,
                                           EnhancedSourceCoordinator coordinator) {
        TokenPaginationCrawlerLeaderProgressState leaderProgressState =
                (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        String oldToken = leaderProgressState.getLastToken();
        leaderProgressState.setLastToken(updatedToken);
        leaderPartition.setLeaderProgressState(leaderProgressState);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_LEASE_DURATION);
        log.info("Updated leader progress state: old lastToken={}, new lastToken={}", oldToken, updatedToken);
    }
}
