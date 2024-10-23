/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_SDK_TIMEOUT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.REQUEST_RESPONSE;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class LambdaProcessorConfig {
    //Ensures 1:1 mapping of events input to lambda and response from lambda
    public static final String STRICT = "strict";
    //When #input events to lambda is not equal to the response from lambda
    public static final String AGGREGATE = "aggregate";

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonPropertyDescription("Lambda Function Name")
    @JsonProperty("function_name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonPropertyDescription("Total retries we want before failing")
    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonPropertyDescription("invocation type defines the way we want to call lambda function")
    @JsonProperty("invocation_type")
    private String invocationType = REQUEST_RESPONSE;

    @JsonPropertyDescription("sdk timeout defines the time sdk maintains the connection to the client before timing out")
    @JsonProperty("sdk_timeout")
    private Duration sdkTimeout = DEFAULT_SDK_TIMEOUT;

    @JsonPropertyDescription("mode defines the way dataprepper treats the response from lambda")
    @JsonProperty("response_processing_mode")
    private String responseProcessingMode = STRICT;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonPropertyDescription("defines a condition for event to use this processor")
    @JsonProperty("lambda_when")
    private String whenCondition;

    @JsonPropertyDescription("Codec configuration for parsing Lambda responses")
    @JsonProperty("response_codec")
    @Valid
    @Nullable
    private PluginModel responseCodecConfig;

    @JsonProperty("tags_on_match_failure")
    @JsonPropertyDescription("A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when lambda fails to " +
            "or exception occurs. This tag may be used in conditional expressions in " +
            "other parts of the configuration")
    private List<String> tagsOnMatchFailure = Collections.emptyList();

    // Getter for codecConfig
    public PluginModel getResponseCodecConfig() {
        return responseCodecConfig;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public BatchOptions getBatchOptions(){return batchOptions;}

    public List<String> getTagsOnMatchFailure(){
        return tagsOnMatchFailure;
    }
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

    public Duration getSdkTimeout() { return sdkTimeout;}

    public String getResponseProcessingMode() {
        return responseProcessingMode;
    }

    public void validateInvocationType() {
        //EVENT type will soon be supported.
        if (!getInvocationType().equals(LambdaCommonConfig.REQUEST_RESPONSE)) {
            throw new IllegalArgumentException("Unsupported invocation type " + getInvocationType());
        }
    }

    public void validateResponseProcessingMode(){
        if(!getResponseProcessingMode().equals(STRICT) &&
        !getResponseProcessingMode().equals(AGGREGATE)){
            throw new IllegalArgumentException("Response processing mode not supported:" + getResponseProcessingMode());
        }
    }
}