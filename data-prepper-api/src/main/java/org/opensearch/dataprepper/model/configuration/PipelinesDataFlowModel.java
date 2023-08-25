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
import com.fasterxml.jackson.annotation.JsonSetter;

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

    @JsonSetter
    public void setVersion(final String version) {
        this.version = DataPrepperVersion.parse(version);
    }
}
