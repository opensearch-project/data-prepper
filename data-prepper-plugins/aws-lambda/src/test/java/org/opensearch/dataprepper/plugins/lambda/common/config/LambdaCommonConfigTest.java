/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
public class LambdaCommonConfigTest {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void test_defaults() {
        final LambdaCommonConfig lambdaCommonConfig = new LambdaCommonConfig();
        assertThat(lambdaCommonConfig.getFunctionName(), equalTo(null));
        assertThat(lambdaCommonConfig.getAwsAuthenticationOptions(), equalTo(null));
        assertThat(lambdaCommonConfig.getBatchOptions(), equalTo(null));
        assertThat(lambdaCommonConfig.getResponseCodecConfig(), equalTo(null));
        assertThat(lambdaCommonConfig.getConnectionTimeout(), equalTo(LambdaCommonConfig.DEFAULT_CONNECTION_TIMEOUT));
        assertThat(lambdaCommonConfig.getMaxConnectionRetries(), equalTo(LambdaCommonConfig.DEFAULT_CONNECTION_RETRIES));
        assertThat(lambdaCommonConfig.getInvocationType(), equalTo(InvocationType.REQUEST_RESPONSE));
    }

    @Test
    public void testAwsAuthenticationOptionsNotNull() throws JsonProcessingException {
        final String config = "        function_name: test_function\n" + "        aws:\n" + "          region: ap-south-1\n" + "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" + "          sts_header_overrides: {\"test\":\"test\"}\n" + "        max_retries: 10\n";
        final LambdaCommonConfig lambdaCommonConfig = objectMapper.readValue(config, LambdaCommonConfig.class);

        assertThat(lambdaCommonConfig.getMaxConnectionRetries(), equalTo(10));
        assertThat(lambdaCommonConfig.getAwsAuthenticationOptions().getAwsRegion(), equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaCommonConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(), equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaCommonConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"), equalTo("test"));
    }
}
