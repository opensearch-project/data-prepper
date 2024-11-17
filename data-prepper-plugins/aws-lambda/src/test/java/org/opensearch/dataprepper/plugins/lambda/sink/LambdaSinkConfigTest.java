/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;


class LambdaSinkConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        this.objectMapper = new ObjectMapper(new YAMLFactory());
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Test
    void test_defaults(){
        MatcherAssert.assertThat(new LambdaSinkConfig().getDlq(),equalTo(null));
    }

    @Test
    void lambda_sink_pipeline_config_test_with_client_options() throws JsonProcessingException {
        final String config =
                "function_name: test_function\n" +
                        "aws:\n" +
                        "  region: ap-south-1\n" +
                        "  sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "  sts_header_overrides: {\"test\":\"test\"}\n" +
                        "client:\n" +
                        "  max_retries: 5\n" +
                        "  api_call_timeout: PT120S\n" +
                        "  connection_timeout: PT30S\n" +
                        "  max_concurrency: 150\n" +
                        "  max_backoff: PT30S\n";

        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);

        // Assert AWS authentication options
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(), equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(), equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"), equalTo("test"));

        // Assert ClientOptions
        ClientOptions clientOptions = lambdaSinkConfig.getClientOptions();
        assertThat(clientOptions.getMaxConnectionRetries(), equalTo(5));
        assertThat(clientOptions.getApiCallTimeout(), equalTo(Duration.ofSeconds(120)));
        assertThat(clientOptions.getConnectionTimeout(), equalTo(Duration.ofSeconds(30)));
        assertThat(clientOptions.getMaxConcurrency(), equalTo(150));
        assertThat(clientOptions.getMaxBackoff(), equalTo(Duration.ofSeconds(30)));
    }


    @Test
    void lambda_sink_pipeline_config_test() throws JsonProcessingException {
        final String config =
                "        function_name: test_function\n" +
                "        aws:\n" +
                "          region: ap-south-1\n" +
                "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                "          sts_header_overrides: {\"test\":\"test\"}\n" +
                "        dlq:\n" +
                "          s3:\n" +
                "            bucket: test\n" +
                "            key_path_prefix: test\n" +
                "            region: ap-south-1\n" +
                "            sts_role_arn: test-role-arn\n";
        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(lambdaSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(lambdaSinkConfig.getDlqStsRoleARN(),equalTo("test-role-arn"));
    }

    @Test
    void lambda_sink_pipeline_config_test_with_no_explicit_aws_config() throws JsonProcessingException {
        final String config =
                        "        function_name: test_function\n" +
                        "        aws:\n" +
                        "          region: ap-south-1\n" +
                        "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "          sts_header_overrides: {\"test\":\"test\"}\n" +
                        "        dlq:\n" +
                        "          s3:\n" +
                        "            bucket: test\n" +
                        "            key_path_prefix: test\n";
        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);

        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(lambdaSinkConfig.getDlqStsRegion(),equalTo("ap-south-1"));
        assertThat(lambdaSinkConfig.getDlqStsRoleARN(),equalTo("arn:aws:iam::524239988912:role/app-test"));
    }

    @Test
    void lambda_sink_pipeline_config_test_with_no_dlq() throws JsonProcessingException {
        final String config =
                "        function_name: test_function\n" +
                        "        aws:\n" +
                        "          region: ap-south-1\n" +
                        "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "          sts_header_overrides: {\"test\":\"test\"}\n" ;
        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        assertThat(lambdaSinkConfig.getDlq(),equalTo(null));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion(),equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(),equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(lambdaSinkConfig.getDlqStsRegion(),equalTo(null));
        assertThat(lambdaSinkConfig.getDlqStsRoleARN(),equalTo(null));
        assertThat(lambdaSinkConfig.getDlqPluginSetting(),equalTo(null));
    }
}
