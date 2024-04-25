/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;

import java.util.HashMap;
import java.util.Map;

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
    public PipelineTemplateModel() {
    }

    public PipelineTemplateModel(final Map<String, Object> pipelines) {
        this.pipelines = pipelines;
    }

    public Map<String, Object> getTemplatePipelines() {
        return pipelines;
    }
}
