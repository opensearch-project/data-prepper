package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;

public class LambdaProcessorConfigTest {

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final Duration DEFAULT_SDK_TIMEOUT = Duration.ofSeconds(60);
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void lambda_processor_default_max_connection_retries_test() {
        assertThat(new LambdaProcessorConfig().getMaxConnectionRetries(), equalTo(DEFAULT_MAX_RETRIES));
    }

    @Test
    void lambda_processor_default_sdk_timeout_test() {
        assertThat(new LambdaProcessorConfig().getSdkTimeout(), equalTo(DEFAULT_SDK_TIMEOUT));
    }

    @Test
    public void testAwsAuthenticationOptionsNotNull() throws JsonProcessingException {
        final String config = "        function_name: test_function\n" + "        aws:\n" + "          region: ap-south-1\n" + "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" + "          sts_header_overrides: {\"test\":\"test\"}\n" + "        max_retries: 10\n";
        final LambdaProcessorConfig lambdaProcessorConfig = objectMapper.readValue(config, LambdaProcessorConfig.class);

        assertThat(lambdaProcessorConfig.getMaxConnectionRetries(), equalTo(10));
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsRegion(), equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(), equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"), equalTo("test"));
    }
}