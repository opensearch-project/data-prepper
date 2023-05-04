/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@DataPrepperPlugin(name = "log_generator", pluginType = Source.class, pluginConfigurationType = LogGeneratorSourceConfig.class)
public class LogGeneratorSource implements Source<Record<Event>> {

    private final LogGeneratorSourceConfig sourceConfig;
    private final LogTypeGenerator logTypeGenerator;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean stopGenerating = new AtomicBoolean();
    private final AtomicInteger logsGenerated = new AtomicInteger();

    @DataPrepperPluginConstructor
    public LogGeneratorSource(final LogGeneratorSourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        this.sourceConfig = sourceConfig;
        this.logTypeGenerator = loadLogTypeGenerator(pluginFactory);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    private LogTypeGenerator loadLogTypeGenerator(final PluginFactory pluginFactory) {
        final PluginModel logTypeConfiguration = sourceConfig.getLogType();
        final PluginSetting logTypePluginSetting = new PluginSetting(logTypeConfiguration.getPluginName(), logTypeConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(LogTypeGenerator.class, logTypePluginSetting);
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (!stopGenerating.get() && hasNotReachedLogCount()) {
                try {
                    final Event generatedEvent = logTypeGenerator.generateEvent();
                    buffer.write(new Record<>(generatedEvent), 500);
                    logsGenerated.getAndIncrement();
                } catch (TimeoutException e) {

                }
            }
        }, 0, sourceConfig.getInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        stopGenerating.set(true);

        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
        }
    }

    private boolean hasNotReachedLogCount() {
        if (sourceConfig.getCount() == 0) {
            return true;
        }

        return logsGenerated.get() < sourceConfig.getCount();
    }
}
