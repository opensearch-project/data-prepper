/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3Group;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3GroupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Class responsible for create {@link S3Client} object, check thresholds,
 * get new buffer and write records into buffer.
 */
public class S3SinkService {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkService.class);
    public static final String OBJECTS_SUCCEEDED = "s3SinkObjectsSucceeded";
    public static final String OBJECTS_FAILED = "s3SinkObjectsFailed";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS = "s3SinkObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED = "s3SinkObjectsEventsFailed";

    private static final String CURRENT_S3_GROUPS = "s3SinkNumberOfGroups";

    static final String NUMBER_OF_GROUPS_FORCE_FLUSHED = "s3SinkObjectsForceFlushed";
    static final String S3_OBJECTS_SIZE = "s3SinkObjectSizeBytes";
    private final S3SinkConfig s3SinkConfig;
    private final Lock reentrantLock;
    private final int maxEvents;
    private final ByteCount maxBytes;
    private final Duration maxCollectionDuration;
    private final int maxRetries;
    private final Counter objectsSucceededCounter;
    private final Counter objectsFailedCounter;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final DistributionSummary s3ObjectSizeSummary;

    private final Counter numberOfObjectsForceFlushed;
    private final OutputCodecContext codecContext;
    private final Duration retrySleepTime;

    private final S3GroupManager s3GroupManager;

    /**
     * @param s3SinkConfig  s3 sink related configuration.
     * @param pluginMetrics metrics.
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig,
                         final OutputCodecContext codecContext,
                         final Duration retrySleepTime,
                         final PluginMetrics pluginMetrics,
                         final S3GroupManager s3GroupManager) {
        this.s3SinkConfig = s3SinkConfig;
        this.codecContext = codecContext;
        this.retrySleepTime = retrySleepTime;
        reentrantLock = new ReentrantLock();

        maxEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        maxBytes = s3SinkConfig.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = s3SinkConfig.getThresholdOptions().getEventCollectTimeOut();

        maxRetries = s3SinkConfig.getMaxUploadRetries();

        objectsSucceededCounter = pluginMetrics.counter(OBJECTS_SUCCEEDED);
        objectsFailedCounter = pluginMetrics.counter(OBJECTS_FAILED);
        numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS);
        numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);
        numberOfObjectsForceFlushed = pluginMetrics.counter(NUMBER_OF_GROUPS_FORCE_FLUSHED);
        pluginMetrics.gauge(CURRENT_S3_GROUPS, s3GroupManager, S3GroupManager::getNumberOfGroups);


        this.s3GroupManager = s3GroupManager;
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        // Don't acquire the lock if there's no work to be done
        if (records.isEmpty() && s3GroupManager.hasNoGroups()) {
            return;
        }

        List<Event> failedEvents = new ArrayList<>();
        Exception sampleException = null;
        reentrantLock.lock();
        try {
            final List<CompletableFuture<?>> completableFutures = new ArrayList<>();
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    final S3Group s3Group = s3GroupManager.getOrCreateGroupForEvent(event);
                    final Buffer currentBuffer = s3Group.getBuffer();
                    final OutputCodec codec = s3Group.getOutputCodec();

                    if (currentBuffer.getEventCount() == 0) {
                        codec.start(currentBuffer.getOutputStream(), event, codecContext);
                    }

                    codec.writeEvent(event, currentBuffer.getOutputStream());
                    int count = currentBuffer.getEventCount() + 1;
                    currentBuffer.setEventCount(count);
                    s3Group.addEventHandle(event.getEventHandle());

                    flushToS3IfNeeded(completableFutures, s3Group, false);
                } catch (Exception ex) {
                    if(sampleException == null) {
                        sampleException = ex;
                    }

                    failedEvents.add(event);
                }
            }

            for (final S3Group s3Group : s3GroupManager.getS3GroupEntries()) {
                flushToS3IfNeeded(completableFutures, s3Group, false);
            }

            if (s3SinkConfig.getAggregateThresholdOptions() != null) {
                checkAggregateThresholdsAndFlushIfNeeded(completableFutures);
            }

            if (!completableFutures.isEmpty()) {
                try {
                    CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                            .thenRun(() -> LOG.debug("All {} requests to S3 have completed", completableFutures.size()))
                            .join();
                } catch (final Exception e) {
                    LOG.warn("There was an exception while waiting for all requests to complete", e);
                }

            }
        } finally {
            reentrantLock.unlock();
        }

        if(!failedEvents.isEmpty()) {
            failedEvents
                    .stream()
                    .map(Event::getEventHandle)
                    .forEach(eventHandle -> eventHandle.release(false));
            LOG.error("Unable to add {} events to buffer. Dropping these events. Sample exception provided.", failedEvents.size(), sampleException);
        }
    }

    /**
     * @return whether the flush was attempted
     */
    private boolean flushToS3IfNeeded(final List<CompletableFuture<?>> completableFutures, final S3Group s3Group, final boolean forceFlush) {
        LOG.trace("Flush to S3 check: currentBuffer.size={}, currentBuffer.events={}, currentBuffer.duration={}",
                s3Group.getBuffer().getSize(), s3Group.getBuffer().getEventCount(), s3Group.getBuffer().getDuration());
        if (forceFlush || ThresholdCheck.checkThresholdExceed(s3Group.getBuffer(), maxEvents, maxBytes, maxCollectionDuration)) {

            s3GroupManager.removeGroup(s3Group);
            try {

                s3Group.getOutputCodec().complete(s3Group.getBuffer().getOutputStream());
                String s3Key = s3Group.getBuffer().getKey();
                LOG.info("Writing {} to S3 with {} events and size of {} bytes.",
                        s3Key, s3Group.getBuffer().getEventCount(), s3Group.getBuffer().getSize());

                final Consumer<Boolean> consumeOnGroupCompletion = (success) -> {
                    if (success) {

                        LOG.info("Successfully saved {} to S3.", s3Key);
                        numberOfRecordsSuccessCounter.increment(s3Group.getBuffer().getEventCount());
                        objectsSucceededCounter.increment();
                        s3ObjectSizeSummary.record(s3Group.getBuffer().getSize());
                        s3Group.releaseEventHandles(true);
                    } else {
                        LOG.error("Failed to save {} to S3.", s3Key);
                        numberOfRecordsFailedCounter.increment(s3Group.getBuffer().getEventCount());
                        objectsFailedCounter.increment();
                        s3Group.releaseEventHandles(false);
                    }
                };

                final Optional<CompletableFuture<?>> completableFuture = s3Group.getBuffer().flushToS3(consumeOnGroupCompletion, this::handleFailures);
                completableFuture.ifPresent(completableFutures::add);

                return true;
            } catch (final IOException e) {
                LOG.error("Exception while completing codec", e);
            }
        }

        return false;
    }

    private void handleFailures(final Throwable e) {
        LOG.error("Exception occurred while uploading records to s3 bucket: {}", e.getMessage());
    }

    private void checkAggregateThresholdsAndFlushIfNeeded(final List<CompletableFuture<?>> completableFutures) {
        long currentTotalGroupSize = s3GroupManager.recalculateAndGetGroupSize();
        LOG.debug("Total groups size is {} bytes", currentTotalGroupSize);

        final long aggregateThresholdBytes = s3SinkConfig.getAggregateThresholdOptions().getMaximumSize().getBytes();
        final double aggregateThresholdFlushRatio = s3SinkConfig.getAggregateThresholdOptions().getFlushCapacityRatio();

        if (currentTotalGroupSize >= aggregateThresholdBytes) {
            LOG.info("aggregate_threshold reached, the largest groups will be flushed until {} percent of the maximum size {} is remaining", aggregateThresholdFlushRatio * 100, aggregateThresholdBytes);

            for (final S3Group s3Group : s3GroupManager.getS3GroupsSortedBySize()) {
                LOG.info("Forcing a flush of object with key {} due to aggregate_threshold of {} bytes being reached", s3Group.getBuffer().getKey(), aggregateThresholdBytes);

                final boolean flushed = flushToS3IfNeeded(completableFutures, s3Group, true);
                numberOfObjectsForceFlushed.increment();

                if (flushed) {
                    currentTotalGroupSize -= s3Group.getBuffer().getSize();
                }

                if (currentTotalGroupSize <= aggregateThresholdBytes * aggregateThresholdFlushRatio) {
                    break;
                }
            }
        }
    }
}
