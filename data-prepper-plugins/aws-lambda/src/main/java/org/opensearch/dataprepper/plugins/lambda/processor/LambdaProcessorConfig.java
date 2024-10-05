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
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.BATCH_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_SDK_TIMEOUT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.REQUEST_RESPONSE;

import java.time.Duration;

public class LambdaProcessorConfig {


    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("function_name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("invocation_type")
    private String invocationType = REQUEST_RESPONSE;

    @JsonProperty("payload_model")
    private String payloadModel = BATCH_EVENT;

    @JsonProperty("sdk_timeout")
    private Duration sdkTimeout = DEFAULT_SDK_TIMEOUT;

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

    public String getInvocationType(){return invocationType;}

    public String getWhenCondition() {
        return whenCondition;
    }

    public String getPayloadModel() {
        return payloadModel;
    }

    public Duration getSdkTimeout() { return sdkTimeout;}
}