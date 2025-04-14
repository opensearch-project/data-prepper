package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class LambdaProcessorConfigTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void test_defaults() {
        final LambdaProcessorConfig lambdaProcessorConfig = new LambdaProcessorConfig();
        assertThat(lambdaProcessorConfig.getTagsOnFailure(), equalTo(List.of()));
        assertThat(lambdaProcessorConfig.getWhenCondition(), equalTo(null));
        assertThat(lambdaProcessorConfig.getResponseEventsMatch(), equalTo(false));

        // Test default client options  private static final int DEFAULT_CIRCUIT_BREAKER_RETRIES = 15; // 15 seconds with 1000ms sleep
        //  private static final long DEFAULT_CIRCUIT_BREAKER_WAIT_INTERVAL_MS = 1000;
        assertThat(lambdaProcessorConfig.getClientOptions(), notNullValue());
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConnectionRetries(), equalTo(ClientOptions.DEFAULT_CONNECTION_RETRIES));
        assertThat(lambdaProcessorConfig.getClientOptions().getApiCallTimeout(), equalTo(ClientOptions.DEFAULT_API_TIMEOUT));
        assertThat(lambdaProcessorConfig.getClientOptions().getConnectionTimeout(), equalTo(ClientOptions.DEFAULT_CONNECTION_TIMEOUT));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConcurrency(), equalTo(ClientOptions.DEFAULT_MAXIMUM_CONCURRENCY));
        assertThat(lambdaProcessorConfig.getClientOptions().getBaseDelay(), equalTo(ClientOptions.DEFAULT_BASE_DELAY));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxBackoff(), equalTo(ClientOptions.DEFAULT_MAX_BACKOFF));
        assertThat(lambdaProcessorConfig.getCircuitBreakerRetries(), equalTo(LambdaProcessorConfig.DEFAULT_CIRCUIT_BREAKER_RETRIES));
        assertThat(lambdaProcessorConfig.getCircuitBreakerWaitInterval(), equalTo(LambdaProcessorConfig.DEFAULT_CIRCUIT_BREAKER_WAIT_INTERVAL_MS));
    }

    @Test
    public void testAwsAuthenticationOptionsAndClientOptions() throws JsonProcessingException {
        final String config = "function_name: test_function\n" +
                "aws:\n" +
                "  region: ap-south-1\n" +
                "  sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                "  sts_header_overrides: {\"test\":\"test\"}\n" +
                "client:\n" +
                "  max_retries: 10\n" +
                "  api_call_timeout: 120\n" +
                "  connection_timeout: 30\n" +
                "  max_concurrency: 150\n" +
                "  base_delay: 0.2\n" +
                "  max_backoff: 30\n";
        final LambdaProcessorConfig lambdaProcessorConfig = objectMapper.readValue(config, LambdaProcessorConfig.class);

        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions(), notNullValue());
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsRegion(), equalTo(Region.AP_SOUTH_1));
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(), equalTo("arn:aws:iam::524239988912:role/app-test"));
        assertThat(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides().get("test"), equalTo("test"));

        assertThat(lambdaProcessorConfig.getClientOptions(), notNullValue());
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConnectionRetries(), equalTo(10));
        assertThat(lambdaProcessorConfig.getClientOptions().getApiCallTimeout(), equalTo(Duration.ofSeconds(120)));
        assertThat(lambdaProcessorConfig.getClientOptions().getConnectionTimeout(), equalTo(Duration.ofSeconds(30)));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConcurrency(), equalTo(150));
        assertThat(lambdaProcessorConfig.getClientOptions().getBaseDelay(), equalTo(Duration.ofMillis(200)));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxBackoff(), equalTo(Duration.ofSeconds(30)));
    }

    @Test
    public void testPartialClientOptions() throws JsonProcessingException {
        final String config = "function_name: test_function\n" +
                "aws:\n" +
                "  region: us-west-2\n" +
                "client:\n" +
                "  max_retries: 5\n" +
                "  connection_timeout: 45\n";
        final LambdaProcessorConfig lambdaProcessorConfig = objectMapper.readValue(config, LambdaProcessorConfig.class);

        assertThat(lambdaProcessorConfig.getClientOptions(), notNullValue());
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConnectionRetries(), equalTo(5));
        assertThat(lambdaProcessorConfig.getClientOptions().getConnectionTimeout(), equalTo(Duration.ofSeconds(45)));
        // Assert defaults for unspecified options
        assertThat(lambdaProcessorConfig.getClientOptions().getApiCallTimeout(), equalTo(ClientOptions.DEFAULT_API_TIMEOUT));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxConcurrency(), equalTo(ClientOptions.DEFAULT_MAXIMUM_CONCURRENCY));
        assertThat(lambdaProcessorConfig.getClientOptions().getBaseDelay(), equalTo(ClientOptions.DEFAULT_BASE_DELAY));
        assertThat(lambdaProcessorConfig.getClientOptions().getMaxBackoff(), equalTo(ClientOptions.DEFAULT_MAX_BACKOFF));
    }
}