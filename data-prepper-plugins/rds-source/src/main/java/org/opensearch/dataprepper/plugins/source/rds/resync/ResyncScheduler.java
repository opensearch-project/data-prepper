/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ResyncScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ResyncScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    private static final int DEFAULT_NUM_RESYNC_WORKERS = 1;

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;
    private final QueryManager queryManager;
    private final String s3Prefix;
    private final Buffer<Record<Event>> buffer;
    private final RecordConverter recordConverter;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final ExecutorService resyncExecutor;

    private volatile boolean shutdownRequested = false;

    public ResyncScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsSourceConfig sourceConfig,
                           final QueryManager queryManager,
                           final String s3Prefix,
                           final Buffer<Record<Event>> buffer,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.queryManager = queryManager;
        this.s3Prefix = s3Prefix;
        this.buffer = buffer;
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;

        resyncExecutor = Executors.newFixedThreadPool(DEFAULT_NUM_RESYNC_WORKERS,
                BackgroundThreadFactory.defaultExecutorThreadFactory("rds-source-resync-worker"));
    }

    @Override
    public void run() {
        LOG.debug("Start running Stream Scheduler");
        ResyncPartition resyncPartition = null;
        while (!shutdownRequested && !Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(ResyncPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    LOG.info("Acquired partition to perform resync");

                    resyncPartition = (ResyncPartition) sourcePartition.get();
                    processResyncPartition(resyncPartition);
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ResyncScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }

            } catch (Exception e) {
                LOG.error("Received an exception during resync, backing off and retrying", e);
                if (resyncPartition != null) {
                    sourceCoordinator.giveUpPartition(resyncPartition);
                }

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
        shutdownRequested = true;
    }

    private void processResyncPartition(ResyncPartition resyncPartition) {
        LOG.info("Processing resync partition: {}", resyncPartition.getPartitionKey());

        AcknowledgementSet acknowledgementSet = null;
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result) {
                    sourceCoordinator.completePartition(resyncPartition);
                    LOG.info("Received acknowledgment of completion from sink for resync partition {}", resyncPartition.getPartitionKey());
                } else {
                    LOG.warn("Negative acknowledgment received for resync partition {}, retrying", resyncPartition.getPartitionKey());
                    sourceCoordinator.giveUpPartition(resyncPartition);
                }
            }, sourceConfig.getDataFileAcknowledgmentTimeout());
        }

        final ResyncWorker resyncWorker = ResyncWorker.create(
                resyncPartition, sourceConfig, queryManager, buffer, recordConverter, acknowledgementSet);

        CompletableFuture.runAsync(resyncWorker, resyncExecutor)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("There was an exception while processing a resync partition", ex);
                        sourceCoordinator.giveUpPartition(resyncPartition);
                    } else {
                        LOG.info("Completed processing resync partition {}", resyncPartition.getPartitionKey());
                        if (!sourceConfig.isAcknowledgmentsEnabled()) {
                            sourceCoordinator.completePartition(resyncPartition);
                        }
                        // else ack set will decide whether to complete or give up the partition
                    }
                });
    }
}
