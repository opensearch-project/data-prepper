/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Model for the Pipelines data flow. This has the format of the Data Prepper
 * pipelines.yaml file.
 *
 * @since 1.2
 */
public class PipelinesDataFlowModel {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private DataPrepperVersion version;

    @JsonProperty("pipeline_extensions")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonSetter(nulls = Nulls.SKIP)
    private PipelineExtensions pipelineExtensions;

    @JsonAnySetter
    private Map<String, PipelineModel> pipelines = new HashMap<>();

    /**
     * Jackson will use this constructor.
     */
    @JsonCreator
    @SuppressWarnings("unused")
    private PipelinesDataFlowModel() { }

    public PipelinesDataFlowModel(final Map<String, PipelineModel> pipelines) {
        this.pipelines = pipelines;
    }

    public PipelinesDataFlowModel(final PipelineExtensions pipelineExtensions,
                                  final Map<String, PipelineModel> pipelines) {
        this.pipelineExtensions = pipelineExtensions;
        this.pipelines = pipelines;
    }

    public PipelinesDataFlowModel(final DataPrepperVersion version, final Map<String, PipelineModel> pipelines) {
        this.version = version;
        this.pipelines = pipelines;
    }

    @JsonAnyGetter
    public Map<String, PipelineModel> getPipelines() {
        return pipelines;
    }

    @JsonGetter
    public String getVersion() {
        return Objects.isNull(this.version) ? null : version.toString();
    }

    @JsonIgnore
    public DataPrepperVersion getDataPrepperVersion() {
        return version;
    }

    public PipelineExtensions getPipelineExtensions() {
        return pipelineExtensions;
    }

    @JsonSetter
    public void setVersion(final String version) {
        this.version = DataPrepperVersion.parse(version);
    }
}
