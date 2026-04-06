/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.coordinator.UsesEnhancedSinkCoordination;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.PartitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Function;

@Experimental
@DataPrepperPlugin(name = "iceberg", pluginType = Sink.class, pluginConfigurationType = IcebergSinkConfig.class)
public class IcebergSink extends AbstractSink<Record<Event>> implements UsesEnhancedSinkCoordination {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);

    private final IcebergSinkConfig config;
    private final PluginSetting pluginSetting;
    private final PluginFactory pluginFactory;
    private final ExpressionEvaluator expressionEvaluator;
    private EnhancedSourceCoordinator coordinator;
    private IcebergSinkService sinkService;
    private volatile boolean initialized;

    @DataPrepperPluginConstructor
    public IcebergSink(final PluginSetting pluginSetting,
                       final IcebergSinkConfig config,
                       final PluginFactory pluginFactory,
                       final SinkContext sinkContext,
                       final ExpressionEvaluator expressionEvaluator) {
        super(pluginSetting);
        this.pluginSetting = pluginSetting;
        this.config = config;
        this.pluginFactory = pluginFactory;
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public void setEnhancedSourceCoordinator(final EnhancedSourceCoordinator coordinator) {
        this.coordinator = coordinator;
        this.coordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }

    @Override
    public void doInitialize() {
        try {
            DlqPushHandler dlqPushHandler = null;
            if (config.getDlq() != null) {
                dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting,
                        PluginMetrics.fromPluginSetting(pluginSetting), config.getDlq(),
                        null, null, "iceberg-sink-");
            }
            sinkService = new IcebergSinkService(config, coordinator, expressionEvaluator, dlqPushHandler,
                    PluginMetrics.fromPluginSetting(pluginSetting));
            initialized = true;
        } catch (final Exception e) {
            LOG.error("Failed to initialize Iceberg sink", e);
            throw new RuntimeException("Failed to initialize Iceberg sink", e);
        }
    }

    @Override
    public boolean isReady() {
        return initialized;
    }

    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        sinkService.output(records);
    }

    @Override
    public void shutdown() {
        if (sinkService != null) {
            sinkService.shutdown();
        }
    }
}
