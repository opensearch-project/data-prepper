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
 * Sets up beans and initializes the live capture manager with configuration values.
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
        final LiveCaptureConfiguration liveCaptureConfig = dataPrepperConfiguration.getLiveCaptureConfiguration();
        
        boolean defaultEnabled = liveCaptureConfig != null && liveCaptureConfig.isDefaultEnabled();
        double defaultRate = liveCaptureConfig != null ? liveCaptureConfig.getDefaultRate() : 1.0;

        LiveCaptureManager.initialize(defaultEnabled, defaultRate);
        LOG.info("LiveCaptureManager initialized with default enabled: {}, default rate: {}",
                defaultEnabled, defaultRate);

        // Initialize LiveCaptureOutputManager if sink configuration is present
        if (liveCaptureConfig != null && liveCaptureConfig.getLiveCaptureOutputSinkConfig() != null) {
            initializeLiveCaptureOutputManager(liveCaptureConfig);
            
            // Sync output manager state with live capture default state
            if (defaultEnabled) {
                LiveCaptureOutputManager.getInstance().enable();
            } else {
                LiveCaptureOutputManager.getInstance().disable();
            }
        }
    }

    /**
     * Initializes the LiveCaptureOutputManager with the configured sink.
     */
    private void initializeLiveCaptureOutputManager(final LiveCaptureConfiguration liveCaptureConfig) {
        Object sinkConfig = liveCaptureConfig.getLiveCaptureOutputSinkConfig();
        if (!(sinkConfig instanceof Map)) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        Map<String, Object> sinkConfigMap = (Map<String, Object>) sinkConfig;

        Sink<Record<Event>> eventSink = null;
        for (Map.Entry<String, Object> entry : sinkConfigMap.entrySet()) {
            if (!"entry_threshold".equals(entry.getKey()) && !"batch_size".equals(entry.getKey())) {
                eventSink = createPluginBasedSink(entry.getKey(), entry.getValue());
                break;
            }
        }

        if (eventSink != null) {
            LiveCaptureOutputManager.getInstance().initialize(eventSink);
            LOG.info("LiveCaptureOutputManager initialized with sink: {}", eventSink.getClass().getSimpleName());
        }
    }

    private Sink<Record<Event>> createPluginBasedSink(String sinkType, Object sinkSettings) {
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
        LiveCaptureOutputManager.getInstance().shutdown();
    }

    /**
     * Creates the LiveCaptureHandler bean for handling REST API requests.
     *
     * @param eventFactory the event factory to use
     * @return the LiveCaptureHandler instance
     */
    @Bean
    public LiveCaptureHandler liveCaptureHandler(final EventFactory eventFactory) {
        return new LiveCaptureHandler(eventFactory);
    }

}