/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;

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
