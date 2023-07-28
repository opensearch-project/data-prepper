/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomHeaderOptions {

    @JsonProperty("X-Amzn-SageMaker-Custom-Attributes")
    private String  customAttributes;

    @JsonProperty("X-Amzn-SageMaker-Target-Model")
    private String  targetModel;

    @JsonProperty("X-Amzn-SageMaker-Target-Variant")
    private String  targetVariant;

    @JsonProperty("X-Amzn-SageMaker-Target-Container-Hostname")
    private String  targetContainerHostname;

    @JsonProperty("X-Amzn-SageMaker-Inference-Id")
    private String  inferenceId;

    @JsonProperty("X-Amzn-SageMaker-Enable-Explanations")
    private String  enableExplanations;

    public String getCustomAttributes() {
        return customAttributes;
    }

    public String getTargetModel() {
        return targetModel;
    }

    public String getTargetVariant() {
        return targetVariant;
    }

    public String getTargetContainerHostname() {
        return targetContainerHostname;
    }

    public String getInferenceId() {
        return inferenceId;
    }

    public String getEnableExplanations() {
        return enableExplanations;
    }
}
