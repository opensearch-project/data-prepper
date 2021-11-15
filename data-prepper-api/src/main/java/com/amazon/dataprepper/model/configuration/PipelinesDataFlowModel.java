/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.HashMap;
import java.util.Map;

/**
 * Model for the Pipelines data flow. This has the format of the Data Prepper
 * pipelines.yaml file.
 *
 * @since 1.2
 */
public class PipelinesDataFlowModel {

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

    @JsonAnyGetter
    public Map<String, PipelineModel> getPipelines() {
        return pipelines;
    }
}
