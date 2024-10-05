/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.BATCH_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_SDK_TIMEOUT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.EVENT;
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

    @JsonProperty("function_name")
    @NotEmpty
    @NotNull
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("invocation_type")
    private String invocationType = EVENT;

    @JsonProperty("payload_model")
    private String payloadModel = BATCH_EVENT;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonProperty("lambda_when")
    private String whenCondition;

    @JsonProperty("sdk_timeout")
    private Duration sdkTimeout = DEFAULT_SDK_TIMEOUT;

    public Duration getSdkTimeout(){return sdkTimeout;}

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

    public String getInvocationType(){return invocationType;}

    public String getWhenCondition() {
        return whenCondition;
    }

    public String getPayloadModel() {
        return payloadModel;
    }
}