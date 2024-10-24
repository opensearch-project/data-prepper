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
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_TIMEOUT;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class LambdaProcessorConfig {

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
    private String invocationType;

    @JsonPropertyDescription("Defines the way Data Prepper treats the response from Lambda")
    @JsonProperty("response_cardinality")
    private String responseCardinality;

    @JsonPropertyDescription("sdk timeout defines the time sdk maintains the connection to the client before timing out")
    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonPropertyDescription("defines a condition for event to use this processor")
    @JsonProperty("lambda_when")
    private String whenCondition;

    @JsonPropertyDescription("Codec configuration for parsing Lambda responses")
    @JsonProperty("response_codec")
    @Valid
    private PluginModel responseCodecConfig;

    @JsonProperty("tags_on_match_failure")
    @JsonPropertyDescription("A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when lambda fails to " +
            "or exception occurs. This tag may be used in conditional expressions in " +
            "other parts of the configuration")
    private List<String> tagsOnMatchFailure = Collections.emptyList();

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

    public String getWhenCondition() {
        return whenCondition;
    }

    public Duration getConnectionTimeout() { return connectionTimeout;}

    public InvocationType getInvocationType() {
        return InvocationType.fromStringDefaultsToRequestResponse(invocationType);
    }

    public ResponseCardinality getResponseCardinality() {
        return ResponseCardinality.fromString(responseCardinality);
    }
}