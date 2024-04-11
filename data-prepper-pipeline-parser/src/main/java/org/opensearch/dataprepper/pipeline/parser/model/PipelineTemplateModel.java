package org.opensearch.dataprepper.pipeline.parser.model;

import com.fasterxml.jackson.annotation.*;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;

import java.util.List;
        import java.util.Map;
        import java.util.HashMap;

public class PipelineTemplateModel {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonSetter(nulls = Nulls.SKIP)
    private DataPrepperVersion version;

    @JsonProperty("pipeline_configurations")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonSetter(nulls = Nulls.SKIP)
    private PipelineExtensions pipelineExtensions;

    @JsonAnySetter
    private Map<String, Object> pipelines = new HashMap<>();

    @JsonCreator
    @SuppressWarnings("unused")
    private PipelineTemplateModel() { }

    public PipelineTemplateModel(final Map<String, Object> pipelines) {
        this.pipelines = pipelines;
    }

    public Map<String, Object> getTemplatePipelines() {
        return pipelines;
    }
}
