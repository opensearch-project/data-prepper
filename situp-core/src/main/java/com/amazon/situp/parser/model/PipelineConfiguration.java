package com.amazon.situp.parser.model;

import com.amazon.situp.model.configuration.PluginSetting;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            @JsonProperty("source") final Map<String, Map<String, Object>> source,
            @JsonProperty("buffer") final Map<String, Map<String, Object>> buffer,
            @JsonProperty("processor") final Map<String, Map<String, Object>> processors,
            @JsonProperty("sink") final Map<String, Map<String, Object>> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this.sourcePluginSetting = getSourceFromConfiguration(source);
        this.bufferPluginSetting = getBufferFromConfiguration(buffer);
        this.processorPluginSettings = getProcessorsFromConfiguration(processors);
        this.sinkPluginSettings = getSinksFromConfiguration(sinks);
        this.workers = getWorkersFromConfiguration(workers);
        this.readBatchDelay = getReadBatchDelayFromConfiguration(delay);
    }

    private PluginSetting getSourceFromConfiguration(final Map<String, Map<String, Object>> sourceConfiguration) {
        final PluginSetting sourcePluginSetting = getFirstPluginSettingFromConfiguration(sourceConfiguration,
                SOURCE_COMPONENT);
        if (sourcePluginSetting == null) {
            throw new IllegalArgumentException("Invalid configuration, source is a required component");
        }
        return sourcePluginSetting;
    }

    private PluginSetting getBufferFromConfiguration(final Map<String, Map<String, Object>> bufferConfiguration) {
        return getFirstPluginSettingFromConfiguration(bufferConfiguration, BUFFER_COMPONENT);
    }

    private List<PluginSetting> getProcessorsFromConfiguration(
            final Map<String, Map<String, Object>> processorConfiguration) {
        return getAllPluginSettingsFromConfiguration(processorConfiguration);
    }

    private List<PluginSetting> getSinksFromConfiguration(
            final Map<String, Map<String, Object>> sinkConfiguration) {
        final List<PluginSetting> sinkPluginSettings = getAllPluginSettingsFromConfiguration(sinkConfiguration);
        if (sinkPluginSettings.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration, at least one sink is required");
        }
        return sinkPluginSettings;
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

    private List<PluginSetting> getAllPluginSettingsFromConfiguration(
            final Map<String, Map<String, Object>> configuration) {
        if (configuration == null) {
            return Collections.emptyList();
        }
        final List<PluginSetting> pluginSettings = new ArrayList<>();
        configuration.forEach((name, setting) -> {
            pluginSettings.add(new PluginSetting(name, setting));
        });
        return pluginSettings;
    }

    private PluginSetting getFirstPluginSettingFromConfiguration(final Map<String, Map<String, Object>> configuration,
                                                                 final String component) {
        if (configuration == null) {
            return null;
        }
        if (configuration.size() > 1) {
            throw new IllegalArgumentException(format("Incorrect configuration for component %s, " +
                    "maximum allowed plugins are 1", component));
        }
        PluginSetting pluginSetting = null;
        for (String key : configuration.keySet()) {
            pluginSetting = new PluginSetting(key, configuration.get(key));
        }
        return pluginSetting;
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
