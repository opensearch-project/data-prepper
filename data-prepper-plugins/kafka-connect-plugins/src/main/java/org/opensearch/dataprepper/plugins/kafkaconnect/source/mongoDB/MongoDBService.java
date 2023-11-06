/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MongoDBService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBService.class);
    private static final Duration EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    private final PluginMetrics pluginMetrics;
    private final MongoDBConfig mongoDBConfig;
    private final Buffer<Record<Object>> buffer;
    private final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;
    private final ScheduledExecutorService scheduledExecutorService;
    private final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private MongoDBSnapshotWorker snapshotWorker;
    private ScheduledFuture<?> snapshotWorkerFuture;


    private MongoDBService(
            final MongoDBConfig mongoDBConfig,
            final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator,
            final Buffer<Record<Object>> buffer,
            final ScheduledExecutorService scheduledExecutorService,
            final AcknowledgementSetManager acknowledgementSetManager,
            final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.mongoDBConfig = mongoDBConfig;
        this.buffer = buffer;
        this.scheduledExecutorService = scheduledExecutorService;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
        this.mongoDBPartitionCreationSupplier = new MongoDBPartitionCreationSupplier(mongoDBConfig);
    }

    public static MongoDBService create(
            final MongoDBConfig mongoDBConfig,
            final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator,
            final Buffer<Record<Object>> buffer,
            final AcknowledgementSetManager acknowledgementSetManager,
            final PluginMetrics pluginMetrics) {
        return new MongoDBService(
                mongoDBConfig,
                sourceCoordinator,
                buffer,
                Executors.newSingleThreadScheduledExecutor(),
                acknowledgementSetManager,
                pluginMetrics);
    }

    public void start() {
        snapshotWorker = new MongoDBSnapshotWorker(
                sourceCoordinator,
                buffer,
                mongoDBPartitionCreationSupplier,
                pluginMetrics,
                acknowledgementSetManager,
                mongoDBConfig);
        snapshotWorkerFuture = scheduledExecutorService.schedule(() -> snapshotWorker.run(), 0L, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdown();
        try {
            snapshotWorkerFuture.cancel(true);
            if (scheduledExecutorService.awaitTermination(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                LOG.info("Successfully waited for the snapshot worker to terminate");
            } else {
                LOG.warn("snapshot worker did not terminate in time, forcing termination");
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for the snapshot worker to terminate", e);
            scheduledExecutorService.shutdownNow();
        }

    }
}
