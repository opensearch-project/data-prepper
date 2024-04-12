/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.documentdb;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.mongo.export.ExportWorker;
import org.opensearch.dataprepper.plugins.mongo.export.MongoDBExportPartitionSupplier;
import org.opensearch.dataprepper.plugins.mongo.stream.StreamScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class MongoTasksRefresher implements PluginConfigObserver<MongoDBSourceConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoTasksRefresher.class);
    static final String CREDENTIALS_CHANGED = "credentialsChanged";
    static final String EXECUTOR_REFRESH_ERRORS = "executorRefreshErrors";
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Buffer<Record<Event>> buffer;
    private final Function<Integer, ExecutorService> executorServiceFunction;
    private final Counter credentialsChangeCounter;
    private final Counter executorRefreshErrorsCounter;
    private MongoDBExportPartitionSupplier currentMongoDBExportPartitionSupplier;
    private MongoDBSourceConfig currentMongoDBSourceConfig;
    private ExecutorService currentExecutor;

    public MongoTasksRefresher(final Buffer<Record<Event>> buffer,
                               final EnhancedSourceCoordinator sourceCoordinator,
                               final PluginMetrics pluginMetrics,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final Function<Integer, ExecutorService> executorServiceFunction) {
        this.sourceCoordinator = sourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.buffer = buffer;
        this.executorServiceFunction = executorServiceFunction;
        credentialsChangeCounter = pluginMetrics.counter(CREDENTIALS_CHANGED);
        executorRefreshErrorsCounter = pluginMetrics.counter(EXECUTOR_REFRESH_ERRORS);
    }

    public void initialize(final MongoDBSourceConfig sourceConfig) {
        this.currentMongoDBSourceConfig = sourceConfig;
        refreshJobs(sourceConfig);
    }

    @Override
    public void update(MongoDBSourceConfig pluginConfig) {
        final MongoDBSourceConfig.CredentialsConfig newCredentialsConfig = pluginConfig.getCredentialsConfig();
        if (basicAuthChanged(newCredentialsConfig)) {
            credentialsChangeCounter.increment();
            try {
                currentExecutor.shutdownNow();
                refreshJobs(pluginConfig);
                currentMongoDBSourceConfig = pluginConfig;
            } catch (Exception e) {
                executorRefreshErrorsCounter.increment();
                LOG.error("Refreshing executor failed.", e);
            }
        }
    }

    private void refreshJobs(MongoDBSourceConfig pluginConfig) {
        final List<Runnable> runnables = new ArrayList<>();
        if (pluginConfig.getCollections().stream().anyMatch(CollectionConfig::isExportEnabled)) {
            currentMongoDBExportPartitionSupplier = new MongoDBExportPartitionSupplier(pluginConfig);
            runnables.add(new ExportScheduler(sourceCoordinator, currentMongoDBExportPartitionSupplier, pluginMetrics));
            runnables.add(new ExportWorker(
                    sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, pluginConfig));
        }
        if (pluginConfig.getCollections().stream().anyMatch(CollectionConfig::isStreamEnabled)) {
            runnables.add(new StreamScheduler(
                    sourceCoordinator, buffer, acknowledgementSetManager, pluginConfig, pluginMetrics));
        }
        this.currentExecutor = executorServiceFunction.apply(runnables.size());
        runnables.forEach(currentExecutor::submit);
    }

    private boolean basicAuthChanged(final MongoDBSourceConfig.CredentialsConfig newCredentialsConfig) {
        final MongoDBSourceConfig.CredentialsConfig currentCredentialsConfig = currentMongoDBSourceConfig
                .getCredentialsConfig();
        return !Objects.equals(currentCredentialsConfig.getUsername(), newCredentialsConfig.getUsername()) ||
                !Objects.equals(currentCredentialsConfig.getPassword(), newCredentialsConfig.getPassword());
    }
}
