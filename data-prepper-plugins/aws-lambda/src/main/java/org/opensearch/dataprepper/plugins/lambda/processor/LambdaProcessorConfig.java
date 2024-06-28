/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;

public class LambdaProcessorConfig {

    public static final String SYNCHRONOUS_MODE = "RequestResponse";
    public static final String ASYNCHRONOUS_MODE = "Event";
    private static final int DEFAULT_CONNECTION_RETRIES = 3;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("function_name")
    @NotEmpty
    @NotNull
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("mode")
    private String mode = SYNCHRONOUS_MODE;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonProperty("lambda_when")
    private String whenCondition;

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public BatchOptions getBatchOptions(){return batchOptions;}

    public String getFunctionName() {
        return functionName;
    }

    public int getMaxConnectionRetries() {
        return maxConnectionRetries;
    }

    public String getMode(){return mode;}

    public String getWhenCondition() {
        return whenCondition;
    }
}