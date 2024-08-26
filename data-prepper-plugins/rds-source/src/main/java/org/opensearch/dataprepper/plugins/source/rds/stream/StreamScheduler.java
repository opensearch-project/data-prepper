/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class StreamScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;
    private final BinaryLogClient binaryLogClient;
    private final Buffer<Record<Event>> buffer;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final ExecutorService executorService;

    private volatile boolean shutdownRequested = false;

    public StreamScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsSourceConfig sourceConfig,
                           final BinaryLogClient binaryLogClient,
                           final Buffer<Record<Event>> buffer,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.binaryLogClient = binaryLogClient;
        this.buffer = buffer;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        LOG.debug("Start running Stream Scheduler");
        while (!shutdownRequested && !Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    LOG.info("Acquired partition to read from stream");

                    final StreamPartition streamPartition = (StreamPartition) sourcePartition.get();
                    final StreamCheckpointer streamCheckpointer = new StreamCheckpointer(sourceCoordinator, streamPartition, pluginMetrics);
                    binaryLogClient.registerEventListener(new BinlogEventListener(
                            buffer, sourceConfig, pluginMetrics, binaryLogClient, streamCheckpointer, acknowledgementSetManager));
                    final StreamWorker streamWorker = StreamWorker.create(sourceCoordinator, binaryLogClient, pluginMetrics);
                    executorService.submit(() -> streamWorker.processStream(streamPartition));
                }

                try {
                    LOG.debug("Looping to acquire new stream partition or idle while stream worker is working");
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }

            } catch (Exception e) {
                LOG.error("Received an exception during stream processing, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
    }

    public void shutdown() {
        executorService.shutdownNow();
        shutdownRequested = true;
    }
}
