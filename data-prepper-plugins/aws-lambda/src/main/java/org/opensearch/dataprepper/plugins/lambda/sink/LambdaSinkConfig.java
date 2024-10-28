/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink;

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
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.STS_REGION;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.STS_ROLE_ARN;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class LambdaSinkConfig {

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonPropertyDescription("Lambda Function Name")
    @JsonProperty("function_name")
    @NotEmpty
    @NotNull
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonPropertyDescription("Total retries we want before failing")
    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonPropertyDescription("invocation type defines the way we want to call lambda function")
    @JsonProperty("invocation_type")
    private InvocationType invocationType = InvocationType.EVENT;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonPropertyDescription("defines a condition for event to use this processor")
    @JsonProperty("lambda_when")
    private String whenCondition;

    @JsonPropertyDescription("sdk timeout defines the time sdk maintains the connection to the client before timing out")
    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    public Duration getConnectionTimeout(){return connectionTimeout;}

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

    public PluginModel getDlq() {
        return dlq;
    }

    public String getDlqStsRoleARN(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_ROLE_ARN)) ?
                String.valueOf(getDlqPluginSetting().get(STS_ROLE_ARN)) :
                awsAuthenticationOptions.getAwsStsRoleArn();
    }

    public String getDlqStsRegion(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_REGION)) ?
                String.valueOf(getDlqPluginSetting().get(STS_REGION)) :
                awsAuthenticationOptions.getAwsRegion().toString();
    }

    public  Map<String, Object> getDlqPluginSetting(){
        return dlq != null ? dlq.getPluginSettings() : Map.of();
    }

    public InvocationType getInvocationType() {
        return invocationType;
    }

    public String getWhenCondition() {
        return whenCondition;
    }

}