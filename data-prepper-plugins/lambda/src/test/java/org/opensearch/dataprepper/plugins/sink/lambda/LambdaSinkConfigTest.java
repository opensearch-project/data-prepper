/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.lambda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


class LambdaSinkConfigTest {
    public static final int DEFAULT_MAX_RETRIES = 3;
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void lambda_sink_default_max_connection_retries_test(){
        assertThat(new LambdaSinkConfig().getMaxConnectionRetries(),equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void lambda_sink_pipeline_config_test() throws JsonProcessingException {
        final String config =
                "        function_name: test_function\n" +
                "        aws:\n" +
                "          region: ap-south-1\n" +
                "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                "          sts_header_overrides: {\"test\":\"test\"}\n" +
                "        max_retries: 10\n" +
                "        dlq:\n" +
                "          s3:\n" +
                "            bucket: test\n" +
                "            key_path_prefix: test\n" +
                "            region: ap-south-1\n" +
                "            sts_role_arn: test-role-arn\n";
        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        assertThat(lambdaSinkConfig.getMaxConnectionRetries(),equalTo(10));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(lambdaSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(lambdaSinkConfig.getDlqStsRoleARN(),equalTo("test-role-arn"));
    }

    @Test
    void lambda_sink_pipeline_config_test_with_no_dlq() throws JsonProcessingException {
        final String config =
                "        function_name: test_function\n" +
                        "        aws:\n" +
                        "          region: ap-south-1\n" +
                        "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "          sts_header_overrides: {\"test\":\"test\"}\n" +
                        "        max_retries: 10\n";
        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        assertThat(lambdaSinkConfig.getMaxConnectionRetries(),equalTo(10));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(lambdaSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(lambdaSinkConfig.getDlqStsRoleARN(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getDlqPluginSetting().get("key"),equalTo(null));
    }
}
