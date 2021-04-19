/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PipelineConfiguration {
    private static final String WORKERS_COMPONENT = "workers";
    private static final String DELAY_COMPONENT = "delay";
    private static final int DEFAULT_READ_BATCH_DELAY = 3_000;
    private static final int DEFAULT_WORKERS = 1;

    private final PluginSetting sourcePluginSetting;
    private final PluginSetting bufferPluginSetting;
    private final List<PluginSetting> prepperPluginSettings;
    private final List<PluginSetting> sinkPluginSettings;
    private final Integer workers;
    private final Integer readBatchDelay;

    @JsonCreator
    public PipelineConfiguration(
            @JsonProperty("source") final Map.Entry<String, Map<String, Object>> source,
            @JsonProperty("buffer") final Map.Entry<String, Map<String, Object>> buffer,
            @JsonProperty("prepper") final List<Map.Entry<String, Map<String, Object>>> preppers,
            @JsonProperty("sink") final List<Map.Entry<String, Map<String, Object>>> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this.sourcePluginSetting = getSourceFromConfiguration(source);
        this.bufferPluginSetting = getBufferFromConfigurationOrDefault(buffer);
        this.prepperPluginSettings = getPreppersFromConfiguration(preppers);
        this.sinkPluginSettings = getSinksFromConfiguration(sinks);
        this.workers = getWorkersFromConfiguration(workers);
        this.readBatchDelay = getReadBatchDelayFromConfiguration(delay);
    }

    public PluginSetting getSourcePluginSetting() {
        return sourcePluginSetting;
    }

    public PluginSetting getBufferPluginSetting() {
        return bufferPluginSetting;
    }

    public List<PluginSetting> getPrepperPluginSettings() {
        return prepperPluginSettings;
    }

    public List<PluginSetting> getSinkPluginSettings() {
        return sinkPluginSettings;
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
        prepperPluginSettings.forEach(prepperPluginSettings ->
                updatePluginSetting(prepperPluginSettings, pipelineName));
        sinkPluginSettings.forEach(sinkPluginSettings ->
                updatePluginSetting(sinkPluginSettings, pipelineName));
    }

    private void updatePluginSetting(
            final PluginSetting pluginSetting, final String pipelineName) {
        pluginSetting.setPipelineName(pipelineName);
        pluginSetting.setProcessWorkers(this.workers);
    }

    private PluginSetting getSourceFromConfiguration(final Map.Entry<String, Map<String, Object>> sourceConfiguration) {
        if (sourceConfiguration == null) {
            throw new IllegalArgumentException("Invalid configuration, source is a required component");
        }
        return getPluginSettingFromConfiguration(sourceConfiguration);
    }

    private PluginSetting getBufferFromConfigurationOrDefault(
            final Map.Entry<String, Map<String, Object>> bufferConfiguration) {
        if (bufferConfiguration == null) {
            return BlockingBuffer.getDefaultPluginSettings();
        }
        return getPluginSettingFromConfiguration(bufferConfiguration);
    }

    private List<PluginSetting> getSinksFromConfiguration(
            final List<Map.Entry<String, Map<String, Object>>> sinkConfigurations) {
        if (sinkConfigurations == null || sinkConfigurations.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration, at least one sink is required");
        }
        return sinkConfigurations.stream().map(PipelineConfiguration::getPluginSettingFromConfiguration)
                .collect(Collectors.toList());
    }

    private List<PluginSetting> getPreppersFromConfiguration(
            final List<Map.Entry<String, Map<String, Object>>> prepperConfigurations) {
        if (prepperConfigurations == null || prepperConfigurations.isEmpty()) {
            return Collections.emptyList();
        }
        return prepperConfigurations.stream().map(PipelineConfiguration::getPluginSettingFromConfiguration)
                .collect(Collectors.toList());
    }


    private static PluginSetting getPluginSettingFromConfiguration(
            final Map.Entry<String, Map<String, Object>> configuration) {
        final Map<String, Object> settingsMap = configuration.getValue();
        //PluginSettings is required to update pipeline name
        return new PluginSetting(configuration.getKey(), settingsMap == null ? new HashMap<>() : settingsMap);
    }

    private Integer getWorkersFromConfiguration(final Integer workersConfiguration) {
        final Integer configuredWorkers = getValueFromConfiguration(workersConfiguration, WORKERS_COMPONENT);
        return configuredWorkers == null ? DEFAULT_WORKERS : configuredWorkers;
    }

    private Integer getReadBatchDelayFromConfiguration(final Integer delayConfiguration) {
        final Integer configuredDelay = getValueFromConfiguration(delayConfiguration, DELAY_COMPONENT);
        return configuredDelay == null ? DEFAULT_READ_BATCH_DELAY : configuredDelay;
    }

    private Integer getValueFromConfiguration(final Integer configuration, final String component) {
        if (configuration != null && configuration <= 0) {
            throw new IllegalArgumentException(format("Invalid configuration, %s cannot be %s",
                    component, configuration));
        }
        return configuration;
    }
}