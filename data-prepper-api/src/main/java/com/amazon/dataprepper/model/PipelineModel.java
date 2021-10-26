package com.amazon.dataprepper.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PipelineModel {

    private static final int DEFAULT_WORKERS = 1;
    private static final String WORKERS_COMPONENT = "workers";
    private static final int DEFAULT_READ_BATCH_DELAY = 3_000;
    private static final String DELAY_COMPONENT = "delay";

    private final PluginModel sourcePluginModel;
    private final List<PluginModel> prepperPluginModel;
    private final List<PluginModel> sinkPluginModel;
    private final Integer workers;
    private final Integer readBatchDelay;

    @JsonCreator
    public PipelineModel(
            @JsonProperty("source") final PluginModel source,
            @JsonProperty("prepper") final List<PluginModel> preppers,
            @JsonProperty("sink") final List<PluginModel> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this.sourcePluginModel = getSourceFromConfiguration(source);
        this.prepperPluginModel = getPreppersFromConfiguration(preppers);
        this.sinkPluginModel = getSinkFromConfiguration(sinks);
        this.workers = getWorkersFromConfiguration(workers);
        this.readBatchDelay = getReadBatchDelayFromConfiguration(delay);
    }

    public PluginModel getSourcePluginModel() {
        return sourcePluginModel;
    }

    public List<PluginModel> getPrepperPluginModel() {
        return prepperPluginModel;
    }

    public List<PluginModel> getSinkPluginModel() {
        return sinkPluginModel;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }

    private PluginModel getSourceFromConfiguration(final PluginModel sourceConfiguration) {
        if(sourceConfiguration == null) {
            throw new IllegalArgumentException("Invalid configuration, source is a required component");
        }
        return getPluginModelFromConfiguration(sourceConfiguration);
    }

    private List<PluginModel> getPreppersFromConfiguration(final List<PluginModel> prepperConfiguration) {
        if(prepperConfiguration == null || prepperConfiguration.isEmpty()) {
            return Collections.emptyList();
        }
        return prepperConfiguration.stream().map(PipelineModel::getPluginModelFromConfiguration)
                .collect(Collectors.toList());
    }

    private List<PluginModel> getSinkFromConfiguration(final List<PluginModel> sinkConfiguration) {
        if(sinkConfiguration == null || sinkConfiguration.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration, at least 1 sink is required");
        }
        return sinkConfiguration.stream().map(PipelineModel::getPluginModelFromConfiguration)
                .collect(Collectors.toList());
    }

    private static PluginModel getPluginModelFromConfiguration(final PluginModel pluginModel) {
        return new PluginModel(pluginModel.getPluginName(), new HashMap<>());
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
