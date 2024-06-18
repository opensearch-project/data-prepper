/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.common.lambda.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;


public class BatchOptions {

    private static final String DEFAULT_BATCH_KEY = "events";

    @JsonProperty("batch_key")
    private String batchKey = DEFAULT_BATCH_KEY;

    @JsonProperty("threshold")
    @NotNull
    ThresholdOptions thresholdOptions = new ThresholdOptions();

    public String getBatchKey(){return batchKey;}

    public ThresholdOptions getThresholdOptions(){return thresholdOptions;}

}