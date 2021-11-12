package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.Map;

public class PipelinesDataFlowModel {

    @JsonAnySetter
    private Map<String, PipelineModel> pipelines;

    public PipelinesDataFlowModel(final Map<String, PipelineModel> pipelines) {
        this.pipelines = pipelines;
    }

    @JsonAnyGetter
    public Map<String, PipelineModel> getPipelines() {
        return pipelines;
    }

}
