/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;


public class BatchOptions {
    static final String DEFAULT_KEY_NAME = "events";

    @JsonProperty("key_name")
    @Size(min = 1, max = 2048)
    private String keyName = DEFAULT_KEY_NAME;

    @JsonProperty("threshold")
    @NotNull
    ThresholdOptions thresholdOptions = new ThresholdOptions();

    public ThresholdOptions getThresholdOptions(){return thresholdOptions;}

    public String getKeyName() {
        return keyName;
    }

}