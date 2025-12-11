/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class SinkForwardConfig {
    @JsonProperty("pipelines")
    List<String> pipelineNames;

    @JsonProperty("with_metadata")
    Map<String, Object> withMetadata;

    @JsonProperty("with_data")
    Map<String, Object> withData;

    @JsonCreator
    public SinkForwardConfig() {
    }

    @JsonCreator
    public SinkForwardConfig(
        @JsonProperty("pipelines") final List<String> pipelineNames,
        @JsonProperty("with_data") final Map<String, Object> withData,
        @JsonProperty("with_metadata") final Map<String, Object> withMetadata) {
        if (pipelineNames.size() != 1) {
            throw new RuntimeException("Supports only one forwarding pipeline");
        }
        this.pipelineNames = pipelineNames;
        this.withData = withData;
        this.withMetadata = withMetadata;
    }

    public List<String> getPipelineNames() {
        return pipelineNames;
    }

    public Map<String, Object> getWithMetadata() {
        return withMetadata;
    }

    public Map<String, Object> getWithData() {
        return withData;
    }

}

