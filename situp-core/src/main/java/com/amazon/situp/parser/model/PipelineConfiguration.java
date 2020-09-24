package com.amazon.situp.parser.model;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.parser.ParseException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class PipelineConfiguration {

    @NotNull(message = "source settings are missing in provided configuration")
    private final PluginSetting sourcePluginSetting;
    private final PluginSetting bufferPluginSetting;
    private final List<PluginSetting> processorPluginSettings;
    @NotEmpty(message = "sink settings are missing in provided configuration, atleast one sink is required")
    private final List<PluginSetting> sinkPluginSettings;
    private final int workers;
    private final int readBatchDelay;

    @JsonCreator
    public PipelineConfiguration(
            @JsonProperty("source") final Map<String, Map<String, Object>> source,
            @JsonProperty("buffer") final Map<String, Map<String, Object>> buffer,
            @JsonProperty("processor") final Map<String, Map<String, Object>> processors,
            @JsonProperty("sink") final Map<String, Map<String, Object>> sinks,
            @JsonProperty("workers") final int workers,
            @JsonProperty("delay") final int delay) {
        this.sourcePluginSetting = getFirstPluginSettingFromConfiguration(source, "source");
        this.bufferPluginSetting = getFirstPluginSettingFromConfiguration(buffer, "buffer");
        this.processorPluginSettings = getAllPluginSettingsFromConfiguration(processors);
        this.sinkPluginSettings = getAllPluginSettingsFromConfiguration(sinks);
        this.workers = workers;
        this.readBatchDelay = delay;

    }

    private List<PluginSetting> getAllPluginSettingsFromConfiguration(final Map<String, Map<String, Object>> settings) {
        if (settings == null) {
            return null;
        }
        final List<PluginSetting> pluginSettings = new ArrayList<>();
        settings.forEach((name, setting) -> {
            pluginSettings.add(new PluginSetting(name, setting));
        });
        return pluginSettings;
    }

    private PluginSetting getFirstPluginSettingFromConfiguration(final Map<String, Map<String, Object>> settings,
                                                                 final String component) {
        if (settings == null) {
            return null;
        }
        if (settings.size() > 1) {
            throw new ParseException(format("Incorrect Configuration for component %s", component));
        }
        PluginSetting pluginSetting = null;
        for (String key : settings.keySet()) {
            pluginSetting = new PluginSetting(key, settings.get(key));
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

    public int getWorkers() {
        return workers;
    }

    public int getReadBatchDelay() {
        return readBatchDelay;
    }
}
