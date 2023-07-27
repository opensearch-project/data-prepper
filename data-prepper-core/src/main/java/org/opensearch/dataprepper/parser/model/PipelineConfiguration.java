/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineConfiguration {
    private static final String WORKERS_COMPONENT = "workers";
    private static final String DELAY_COMPONENT = "delay";
    private static final int DEFAULT_READ_BATCH_DELAY = 3_000;
    private static final int DEFAULT_WORKERS = 1;

    private final PluginSetting sourcePluginSetting;
    private final PluginSetting bufferPluginSetting;
    private final List<PluginSetting> processorPluginSettings;
    private final List<SinkContextPluginSetting> sinkPluginSettings;

    private final Integer workers;
    private final Integer readBatchDelay;
    private final Set<ConditionalRoute> routes;

    public PipelineConfiguration(final PipelineModel pipelineModel) {
        this.sourcePluginSetting = getSourceFromPluginModel(pipelineModel.getSource());
        this.bufferPluginSetting = getBufferFromPluginModelOrDefault(pipelineModel.getBuffer());
        this.processorPluginSettings = getProcessorsFromPluginModel(pipelineModel.getProcessors());
        this.sinkPluginSettings = getSinksFromPluginModel(pipelineModel.getSinks());
        this.workers = getWorkersFromPipelineModel(pipelineModel);
        this.readBatchDelay = getReadBatchDelayFromPipelineModel(pipelineModel);
        routes = new HashSet<>(pipelineModel.getRoutes());
    }

    public PluginSetting getSourcePluginSetting() {
        return sourcePluginSetting;
    }

    public PluginSetting getBufferPluginSetting() {
        return bufferPluginSetting;
    }

    public List<PluginSetting> getProcessorPluginSettings() {
        return processorPluginSettings;
    }

    public List<SinkContextPluginSetting> getSinkPluginSettings() {
        return sinkPluginSettings;
    }

    public Set<ConditionalRoute> getRoutes() {
        return routes;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }

    public void updateCommonPipelineConfiguration(final String pipelineName) {
        updatePluginSetting(sourcePluginSetting, pipelineName);
        updatePluginSetting(bufferPluginSetting, pipelineName);
        processorPluginSettings.forEach(processorPluginSettings ->
                updatePluginSetting(processorPluginSettings, pipelineName));
        sinkPluginSettings.forEach(sinkPluginSettings ->
                updatePluginSetting(sinkPluginSettings, pipelineName));
    }

    private void updatePluginSetting(
            final PluginSetting pluginSetting, final String pipelineName) {
        pluginSetting.setPipelineName(pipelineName);
        pluginSetting.setProcessWorkers(this.workers);
    }

    private PluginSetting getSourceFromPluginModel(final PluginModel pluginModel) {
        if (pluginModel == null) {
            throw new IllegalArgumentException("Invalid configuration, source is a required component");
        }
        return getPluginSettingFromPluginModel(pluginModel);
    }

    private PluginSetting getBufferFromPluginModelOrDefault(
            final PluginModel pluginModel) {
        if (pluginModel == null) {
            return BlockingBuffer.getDefaultPluginSettings();
        }
        return getPluginSettingFromPluginModel(pluginModel);
    }

    private List<SinkContextPluginSetting> getSinksFromPluginModel(
            final List<SinkModel> sinkConfigurations) {
        if (sinkConfigurations == null || sinkConfigurations.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration, at least one sink is required");
        }
        return sinkConfigurations.stream().map(PipelineConfiguration::getSinkContextPluginSettingFromSinkModel)
                .collect(Collectors.toList());
    }

    private List<PluginSetting> getProcessorsFromPluginModel(
            final List<PluginModel> processorConfigurations) {
        if (processorConfigurations == null || processorConfigurations.isEmpty()) {
            return Collections.emptyList();
        }
        return processorConfigurations.stream().map(PipelineConfiguration::getPluginSettingFromPluginModel)
                .collect(Collectors.toList());
    }


    private static PluginSetting getPluginSettingFromPluginModel(final PluginModel pluginModel) {
        final Map<String, Object> settingsMap = Optional
                .ofNullable(pluginModel.getPluginSettings())
                .orElseGet(HashMap::new);
        return new PluginSetting(pluginModel.getPluginName(), settingsMap);
    }

    private static SinkContextPluginSetting getSinkContextPluginSettingFromSinkModel(final SinkModel sinkModel) {
        final Map<String, Object> settingsMap = Optional
                .ofNullable(sinkModel.getPluginSettings())
                .orElseGet(HashMap::new);
        return new SinkContextPluginSetting(sinkModel.getPluginName(), settingsMap, new SinkContext(sinkModel.getTagsTargetKey(), sinkModel.getRoutes()));
    }

    private Integer getWorkersFromPipelineModel(final PipelineModel pipelineModel) {
        final Integer configuredWorkers = pipelineModel.getWorkers();
        validateConfiguration(configuredWorkers, WORKERS_COMPONENT);
        return configuredWorkers == null ? DEFAULT_WORKERS : configuredWorkers;
    }

    private Integer getReadBatchDelayFromPipelineModel(final PipelineModel pipelineModel) {
        final Integer configuredDelay = pipelineModel.getReadBatchDelay();

        if (configuredDelay != null && configuredDelay < 0) {
            throw new IllegalArgumentException(String.format("Invalid configuration, %s must be a non-negative integer.", DELAY_COMPONENT));
        }

        return configuredDelay == null ? DEFAULT_READ_BATCH_DELAY : configuredDelay;
    }

    private void validateConfiguration(final Integer configuration, final String component) {
        if (configuration != null && configuration <= 0) {
            throw new IllegalArgumentException(String.format("Invalid configuration, %s cannot be %s",
                    component, configuration));
        }
    }
}
