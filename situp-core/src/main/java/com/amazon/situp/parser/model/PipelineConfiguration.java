package com.amazon.situp.parser.model;

import com.amazon.situp.model.configuration.PluginSetting;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PipelineConfiguration {
    private static final String SOURCE_COMPONENT = "source";
    private static final String BUFFER_COMPONENT = "buffer";
    private static final String WORKERS_COMPONENT = "workers";
    private static final String DELAY_COMPONENT = "delay";

    private final PluginSetting sourcePluginSetting;
    private final PluginSetting bufferPluginSetting;
    private final List<PluginSetting> processorPluginSettings;
    private final List<PluginSetting> sinkPluginSettings;
    private final Integer workers;
    private final Integer readBatchDelay;

    @JsonCreator
    public PipelineConfiguration(
            @JsonProperty("source") final Map.Entry<String, Map<String, Object>> source,
            @JsonProperty("buffer") final Map.Entry<String, Map<String, Object>> buffer,
            @JsonProperty("processor") final List<Map.Entry<String, Map<String, Object>>> processors,
            @JsonProperty("sink") final List<Map.Entry<String, Map<String, Object>>> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this.sourcePluginSetting = getSourceFromConfiguration(source);
        this.bufferPluginSetting = getBufferFromConfiguration(buffer);
        this.processorPluginSettings = getProcessorsFromConfiguration(processors);
        this.sinkPluginSettings = getSinksFromConfiguration(sinks);
        this.workers = getWorkersFromConfiguration(workers);
        this.readBatchDelay = getReadBatchDelayFromConfiguration(delay);
    }

    private PluginSetting getSourceFromConfiguration(final Map.Entry<String, Map<String, Object>> sourceConfiguration) {
        if (sourceConfiguration == null) {
            throw new IllegalArgumentException("Invalid configuration, source is a required component");
        }
        return getPluginSettingFromConfiguration(sourceConfiguration);
    }

    private PluginSetting getBufferFromConfiguration(final Map.Entry<String, Map<String, Object>> bufferConfiguration) {
        if(bufferConfiguration==null) {
            return null;
        }
        return getPluginSettingFromConfiguration(bufferConfiguration);
    }

    private List<PluginSetting> getSinksFromConfiguration(
            final List<Map.Entry<String, Map<String, Object>>> sinkConfigurations) {
        if (sinkConfigurations==null || sinkConfigurations.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration, at least one sink is required");
        }
        return sinkConfigurations.stream().map(PipelineConfiguration::getPluginSettingFromConfiguration).collect(Collectors.toList());
    }

    private List<PluginSetting> getProcessorsFromConfiguration(
            final List<Map.Entry<String, Map<String, Object>>> processorConfigurations) {
        if (processorConfigurations==null || processorConfigurations.isEmpty()) {
            return Collections.emptyList();
        }
        return processorConfigurations.stream().map(PipelineConfiguration::getPluginSettingFromConfiguration).collect(Collectors.toList());
    }



    private static PluginSetting getPluginSettingFromConfiguration(
            final Map.Entry<String, Map<String, Object>> processorConfiguration) {
        return new PluginSetting(processorConfiguration.getKey(), processorConfiguration.getValue());
    }

    private Integer getWorkersFromConfiguration(final Integer workersConfiguration) {
        return getValueFromConfiguration(workersConfiguration, WORKERS_COMPONENT);
    }

    private Integer getReadBatchDelayFromConfiguration(final Integer delayConfiguration) {
        return getValueFromConfiguration(delayConfiguration, DELAY_COMPONENT);
    }

    private Integer getValueFromConfiguration(final Integer configuration, final String component) {
        if (configuration != null && configuration <= 0) {
            throw new IllegalArgumentException(format("Invalid configuration, %s cannot be %s",
                    component, configuration));
        }
        return configuration;
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

    public List<PluginSetting> getSinkPluginSettings() {
        return sinkPluginSettings;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }
}
