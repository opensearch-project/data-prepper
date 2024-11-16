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
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;

import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.DEFAULT_CONNECTION_TIMEOUT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.STS_REGION;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.STS_ROLE_ARN;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class LambdaSinkConfig extends LambdaCommonConfig {

    @JsonProperty("dlq")
    private PluginModel dlq;

    public PluginModel getDlq() {
        return dlq;
    }

    public String getDlqStsRoleARN(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_ROLE_ARN)) ?
                String.valueOf(getDlqPluginSetting().get(STS_ROLE_ARN)) :
                getAwsAuthenticationOptions().getAwsStsRoleArn();
    }

    public String getDlqStsRegion(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_REGION)) ?
                String.valueOf(getDlqPluginSetting().get(STS_REGION)) :
                getAwsAuthenticationOptions().getAwsRegion().toString();
    }

    public  Map<String, Object> getDlqPluginSetting(){
        return dlq != null ? dlq.getPluginSettings() : Map.of();
    }

}
