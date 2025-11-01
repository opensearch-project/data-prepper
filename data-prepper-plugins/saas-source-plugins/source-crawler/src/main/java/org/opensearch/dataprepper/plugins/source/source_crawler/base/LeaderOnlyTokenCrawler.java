package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Named
public class LeaderOnlyTokenCrawler implements Crawler<SaasWorkerProgressState> {
    private static final Logger log = LoggerFactory.getLogger(LeaderOnlyTokenCrawler.class);
    private static final Duration NO_ACK_TIME_OUT_SECONDS = Duration.ofSeconds(900);
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(1);
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(15);
    private static final int BATCH_SIZE = 1000;

    private static final String METRIC_BATCHES_FAILED = "batchesFailed";
    private static final String METRIC_BUFFER_WRITE_TIME = "bufferWriteTime";
    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";

    private final CrawlerClient client;
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
    private boolean shouldStopCrawl = false;
    private Duration noAckTimeout;

    public LeaderOnlyTokenCrawler(
            CrawlerClient client,
            PluginMetrics pluginMetrics) {
        this.client = client;
        this.pluginMetrics = pluginMetrics;
        this.crawlingTimer = pluginMetrics.timer("crawlingTime");
        this.batchesFailedCounter = pluginMetrics.counter(METRIC_BATCHES_FAILED);
        this.bufferWriteTimer = pluginMetrics.timer(METRIC_BUFFER_WRITE_TIME);
        this.acknowledgementSetSuccesses = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME);
        this.acknowledgementSetFailures = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME);
        this.noAckTimeout = NO_ACK_TIME_OUT_SECONDS;
    }

    @Override
    public Instant crawl(LeaderPartition leaderPartition,
                         EnhancedSourceCoordinator coordinator) {
        shouldStopCrawl = false;
        long startTime = System.currentTimeMillis();
        Instant lastCheckpointTime = Instant.now();
        TokenPaginationCrawlerLeaderProgressState leaderProgressState =
                (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        lastToken = leaderProgressState.getLastToken();

        log.info("Starting leader-only crawl with token: {}", lastToken);

        Iterator<ItemInfo> itemIterator = ((LeaderOnlyTokenCrawlerClient) client).listItems(lastToken);

        while (itemIterator.hasNext() && !shouldStopCrawl) {
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
            AtomicBoolean ackReceived = new AtomicBoolean(false);
            long createTimestamp = System.currentTimeMillis();
            AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(
                    success -> {
                        ackReceived.set(true);
                        if (success) {
                            // On success: update checkpoint
                            acknowledgementSetSuccesses.increment();
                            updateLeaderProgressState(leaderPartition, lastToken, coordinator);
                        } else {
                            // On failure: Stop the crawl
                            acknowledgementSetFailures.increment();
                            log.warn("Batch processing received negative acknowledgment for token: {}. Stopping current crawl.", lastToken);
                            shouldStopCrawl = true;
                        }
                    },
                    noAckTimeout
            );

            bufferWriteTimer.record(() -> {
                try {
                    ((LeaderOnlyTokenCrawlerClient) client).writeBatchToBuffer(batch, buffer, acknowledgementSet);
                    acknowledgementSet.complete();
                    // Check every 15 seconds until either:
                    // 1. We get an ack (positive/negative)
                    // 2. Or timeout duration is reached
                    while (!ackReceived.get()) {
                        Thread.sleep(Duration.ofSeconds(15).toMillis());
                        Duration ackWaitDuration = Duration.between(Instant.ofEpochMilli(createTimestamp), Instant.now());

                        if (!ackWaitDuration.minus(noAckTimeout).isNegative()) {
                            // No ack received within NO_ACK_TIME_OUT_SECONDS
                            log.warn("Acknowledgment not received for batch with token {} past wait time. Stopping current crawl.", lastToken);
                            shouldStopCrawl = true;
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for acknowledgment", e);
                } catch (Exception e) {
                    log.error("Failed to process batch ending with token {}", lastToken, e);
                    acknowledgementSet.complete();
                    throw e;
                }
            });
        } else {
            // Without Acknowledgments:
            // Write directly and update checkpoint
            bufferWriteTimer.record(() -> {
                try {
                    ((LeaderOnlyTokenCrawlerClient) client).writeBatchToBuffer(batch, buffer, null);
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

    @VisibleForTesting
    void setNoAckTimeout(Duration timeout) {
        this.noAckTimeout = timeout;
    }
}