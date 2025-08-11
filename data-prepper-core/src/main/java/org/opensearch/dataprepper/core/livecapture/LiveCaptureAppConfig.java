/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Map;

/**
 * Spring configuration for live capture functionality.
 */
@Configuration
public class LiveCaptureAppConfig implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(LiveCaptureAppConfig.class);
    private final DataPrepperConfiguration dataPrepperConfiguration;
    private ApplicationContext applicationContext;

    @Inject
    public LiveCaptureAppConfig(final DataPrepperConfiguration dataPrepperConfiguration) {
        this.dataPrepperConfiguration = dataPrepperConfiguration;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void initializeLiveCaptureManager() {
        final LiveCaptureConfiguration config = dataPrepperConfiguration.getLiveCaptureConfiguration();
        
        boolean enabled = config != null && config.isDefaultEnabled();
        double rate = config != null ? config.getDefaultRate() : 1.0;

        LiveCaptureManager.initialize(enabled, rate);

        if (config != null && config.getLiveCaptureOutputSinkConfig() != null) {
            initializeOutputSink(config);
        }
    }

    private void initializeOutputSink(final LiveCaptureConfiguration config) {
        Object sinkConfig = config.getLiveCaptureOutputSinkConfig();
        if (!(sinkConfig instanceof Map)) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        Map<String, Object> configMap = (Map<String, Object>) sinkConfig;

        // Find the first non-metadata entry to create the sink
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            if (!"entry_threshold".equals(entry.getKey()) && !"batch_size".equals(entry.getKey())) {
                Sink<Record<Event>> sink = createSink(entry.getKey(), entry.getValue());
                if (sink != null) {
                    sink.initialize();
                    LiveCaptureManager.getInstance().setOutputSink(sink);
                }
                break;
            }
        }
    }

    private Sink<Record<Event>> createSink(String sinkType, Object sinkSettings) {
        if (!(sinkSettings instanceof Map)) {
            return null;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> settingsMap = (Map<String, Object>) sinkSettings;

        PluginFactory pluginFactory = applicationContext.getBean(PluginFactory.class);
        PluginSetting pluginSetting = new PluginSetting(sinkType, settingsMap);
        pluginSetting.setPipelineName("live-capture-pipeline");

        @SuppressWarnings("unchecked")
        Sink<Record<Event>> sink = (Sink<Record<Event>>) pluginFactory.loadPlugin(
                Sink.class, pluginSetting, new SinkContext(null));

        return sink;
    }


    @PreDestroy
    public void shutdownLiveCapture() {
        LOG.debug("Live capture shutdown - sink lifecycle managed independently");
    }

    @Bean
    public LiveCaptureHandler liveCaptureHandler(final EventFactory eventFactory) {
        return new LiveCaptureHandler(eventFactory);
    }

}