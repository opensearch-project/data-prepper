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
        this.sourcePluginModel = source;
        this.prepperPluginModel = preppers;
        this.sinkPluginModel = sinks;
        this.workers = workers;
        this.readBatchDelay = delay;
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

}
