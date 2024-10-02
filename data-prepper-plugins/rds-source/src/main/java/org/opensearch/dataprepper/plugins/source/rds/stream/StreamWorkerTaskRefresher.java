/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamWorkerTaskRefresher implements PluginConfigObserver<RdsSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamWorkerTaskRefresher.class);
    static final String CREDENTIALS_CHANGED = "credentialsChanged";
    static final String TASK_REFRESH_ERRORS = "streamWorkerTaskRefreshErrors";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final StreamPartition streamPartition;
    private final StreamCheckpointer streamCheckpointer;
    private final String s3Prefix;
    private final BinlogClientFactory binlogClientFactory;
    private final Buffer<Record<Event>> buffer;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Counter credentialsChangeCounter;
    private final Counter taskRefreshErrorsCounter;

    private ExecutorService executorService;
    private RdsSourceConfig currentSourceConfig;

    public StreamWorkerTaskRefresher(final EnhancedSourceCoordinator sourceCoordinator,
                                     final StreamPartition streamPartition,
                                     final StreamCheckpointer streamCheckpointer,
                                     final String s3Prefix,
                                     final BinlogClientFactory binlogClientFactory,
                                     final Buffer<Record<Event>> buffer,
                                     final AcknowledgementSetManager acknowledgementSetManager,
                                     final ExecutorService executorService,
                                     final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.streamPartition = streamPartition;
        this.streamCheckpointer = streamCheckpointer;
        this.s3Prefix = s3Prefix;
        this.buffer = buffer;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.binlogClientFactory = binlogClientFactory;
        this.executorService = executorService;
        this.credentialsChangeCounter = pluginMetrics.counter(CREDENTIALS_CHANGED);
        this.taskRefreshErrorsCounter = pluginMetrics.counter(TASK_REFRESH_ERRORS);
    }

    public static StreamWorkerTaskRefresher create(final EnhancedSourceCoordinator sourceCoordinator,
                                                   final StreamPartition streamPartition,
                                                   final StreamCheckpointer streamCheckpointer,
                                                   final String s3Prefix,
                                                   final BinlogClientFactory binlogClientFactory,
                                                   final Buffer<Record<Event>> buffer,
                                                   final AcknowledgementSetManager acknowledgementSetManager,
                                                   final ExecutorService executorService,
                                                   final PluginMetrics pluginMetrics) {
        return new StreamWorkerTaskRefresher(sourceCoordinator, streamPartition, streamCheckpointer, s3Prefix,
                binlogClientFactory, buffer, acknowledgementSetManager, executorService, pluginMetrics);
    }

    public void initialize(RdsSourceConfig sourceConfig) {
        currentSourceConfig = sourceConfig;
        refreshTask(sourceConfig);
    }

    @Override
    public void update(RdsSourceConfig sourceConfig) {
        if (basicAuthChanged(sourceConfig.getAuthenticationConfig())) {
            LOG.info("Database credentials were updated. Refreshing stream worker...");
            credentialsChangeCounter.increment();
            try {
                executorService.shutdownNow();
                executorService = Executors.newSingleThreadExecutor();
                binlogClientFactory.setCredentials(
                        sourceConfig.getAuthenticationConfig().getUsername(), sourceConfig.getAuthenticationConfig().getPassword());

                refreshTask(sourceConfig);

                currentSourceConfig = sourceConfig;
            } catch (Exception e) {
                taskRefreshErrorsCounter.increment();
                LOG.error("Refreshing stream worker failed", e);
            }
        }
        LOG.debug("Database credentials were not changed. Skipping...");
    }

    private void refreshTask(RdsSourceConfig sourceConfig) {
        final BinaryLogClient binaryLogClient = binlogClientFactory.create();
        binaryLogClient.registerEventListener(BinlogEventListener.create(
                buffer, sourceConfig, s3Prefix, pluginMetrics, binaryLogClient, streamCheckpointer, acknowledgementSetManager));
        final StreamWorker streamWorker = StreamWorker.create(sourceCoordinator, binaryLogClient, pluginMetrics);
        executorService.submit(() -> streamWorker.processStream(streamPartition));
    }

    private boolean basicAuthChanged(final RdsSourceConfig.AuthenticationConfig newAuthConfig) {
        final RdsSourceConfig.AuthenticationConfig currentAuthConfig = currentSourceConfig.getAuthenticationConfig();
        return !Objects.equals(currentAuthConfig.getUsername(), newAuthConfig.getUsername()) ||
                !Objects.equals(currentAuthConfig.getPassword(), newAuthConfig.getPassword());
    }
}
