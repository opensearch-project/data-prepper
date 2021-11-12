package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Model class for a Pipeline containing all possible Plugin types in Configuration YAML
 *
 * @since 1.2
 */
public class PipelineModel {

    private final PluginModel source;
    private final List<PluginModel> preppers;
    private final List<PluginModel> sinks;
    private final Integer workers;
    private final Integer readBatchDelay;

    @JsonCreator
    public PipelineModel(
            @JsonProperty("source") final PluginModel source,
            @JsonProperty("prepper") final List<PluginModel> preppers,
            @JsonProperty("sink") final List<PluginModel> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this.source = source;
        this.preppers = preppers;
        this.sinks = sinks;
        this.workers = workers;
        this.readBatchDelay = delay;
    }

    public PluginModel getSource() {
        return source;
    }

    public List<PluginModel> getPreppers() {
        return preppers;
    }

    public List<PluginModel> getSinks() {
        return sinks;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }

}
