/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HttpSinkConfigurationTest {

    private static final String SINK_YAML = "        urls:\n" +
            "        - url: \"https://httpbin.org/post\"\n" +
            "          workers: 1\n" +
            "          proxy: test\n" +
            "          codec:\n" +
            "            ndjson:\n" +
            "          http_method: \"POST\"\n" +
            "          auth_type: \"http_basic\"\n" +
            "        proxy: test-proxy\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        http_method: \"POST\"\n" +
            "        auth_type: \"http_basic\"\n" +
            "        authentication:\n" +
            "          http_basic:\n" +
            "            username: \"username\"\n" +
            "            password: \"vip\"\n" +
            "          bearer_token:\n" +
            "            token: \"\"\n" +
            "        ssl: false\n" +
            "        dlq_file: \"/your/local/dlq-file\"\n" +
            "        dlq:\n" +
            "        ssl_certificate_file: \"/full/path/to/certfile.crt\"\n" +
            "        ssl_key_file: \"/full/path/to/keyfile.key\"\n" +
            "        buffer_type: \"in_memory\"\n" +
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        threshold:\n" +
            "          event_count: 2000\n" +
            "          maximum_size: 2mb\n" +
            "        max_retries: 5\n" +
            "        aws_sigv4: true\n" +
            "        custom_header:\n" +
            "          X-Amzn-SageMaker-Custom-Attributes: test-attribute\n" +
            "          X-Amzn-SageMaker-Target-Model: test-target-model\n" +
            "          X-Amzn-SageMaker-Target-Variant: test-target-variant\n" +
            "          X-Amzn-SageMaker-Target-Container-Hostname: test-container-host\n" +
            "          X-Amzn-SageMaker-Inference-Id: test-interface-id\n" +
            "          X-Amzn-SageMaker-Enable-Explanations: test-explanation";

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    @Test
    void default_worker_test() {
        assertThat(new HttpSinkConfiguration().getWorkers(), CoreMatchers.equalTo(1));
    }

    @Test
    void default_codec_test() {
        assertNull(new HttpSinkConfiguration().getCodec());
    }

    @Test
    void default_proxy_test() {
        assertNull(new HttpSinkConfiguration().getProxy());
    }

    @Test
    void default_http_method_test() {
        assertThat(new HttpSinkConfiguration().getHttpMethod(), equalTo(HTTPMethodOptions.POST));
    }

    @Test
    void default_auth_type_test() {
        assertThat(new HttpSinkConfiguration().getAuthType(),equalTo(AuthTypeOptions.UNAUTHENTICATED));
    }

    @Test
    void get_urls_test() {
        assertThat(new HttpSinkConfiguration().getUrlConfigurationOptions(), equalTo(null));
    }

    @Test
    void get_authentication_test() {
        assertNull(new HttpSinkConfiguration().getAuthentication());
    }

    @Test
    void default_insecure_test() {
        assertThat(new HttpSinkConfiguration().isSsl(), equalTo(false));
    }

    @Test
    void default_awsSigv4_test() {
        assertThat(new HttpSinkConfiguration().isAwsSigv4(), equalTo(false));
    }

    @Test
    void get_ssl_certificate_file_test() {
        assertNull(new HttpSinkConfiguration().getSslCertificateFile());
    }

    @Test
    void get_ssl_key_file_test() {
        assertNull(new HttpSinkConfiguration().getSslKeyFile());
    }

    @Test
    void default_buffer_type_test() {
        assertThat(new HttpSinkConfiguration().getBufferType(), equalTo(BufferTypeOptions.IN_MEMORY));
    }

    @Test
    void get_threshold_options_test() {
        assertNull(new HttpSinkConfiguration().getThresholdOptions());
    }

    @Test
    void default_max_upload_retries_test() {
        assertThat(new HttpSinkConfiguration().getMaxUploadRetries(), equalTo(5));
    }

    @Test
    void get_aws_authentication_options_test() {
        assertNull(new HttpSinkConfiguration().getAwsAuthenticationOptions());
    }

    @Test
    void get_custom_header_options_test() {
        assertNull(new HttpSinkConfiguration().getCustomHeaderOptions());
    }

    @Test
    void http_sink_pipeline_test_with_provided_config_options() throws JsonProcessingException {
        final HttpSinkConfiguration httpSinkConfiguration = objectMapper.readValue(SINK_YAML, HttpSinkConfiguration.class);

        assertThat(httpSinkConfiguration.getHttpMethod(),equalTo(HTTPMethodOptions.POST));
        assertThat(httpSinkConfiguration.getAuthType(),equalTo(AuthTypeOptions.HTTP_BASIC));
        assertThat(httpSinkConfiguration.getBufferType(),equalTo(BufferTypeOptions.IN_MEMORY));
        assertThat(httpSinkConfiguration.getMaxUploadRetries(),equalTo(5));
        assertThat(httpSinkConfiguration.getProxy(),equalTo("test-proxy"));
        assertThat(httpSinkConfiguration.getSslCertificateFile(),equalTo("/full/path/to/certfile.crt"));
        assertThat(httpSinkConfiguration.getSslKeyFile(),equalTo("/full/path/to/keyfile.key"));
        assertThat(httpSinkConfiguration.getWorkers(),equalTo(1));
        assertThat(httpSinkConfiguration.getDlqFile(),equalTo("/your/local/dlq-file"));

        final UrlConfigurationOption urlConfigurationOption = httpSinkConfiguration.getUrlConfigurationOptions().get(0);
        assertThat(urlConfigurationOption.getUrl(),equalTo("https://httpbin.org/post"));
        assertThat(urlConfigurationOption.getHttpMethod(),equalTo(HTTPMethodOptions.POST));
        assertThat(urlConfigurationOption.getProxy(),equalTo("test"));
        assertThat(urlConfigurationOption.getAuthType(),equalTo(AuthTypeOptions.HTTP_BASIC));

        final CustomHeaderOptions customHeaderOptions = httpSinkConfiguration.getCustomHeaderOptions();

        assertThat(customHeaderOptions.getCustomAttributes(),equalTo("test-attribute"));
        assertThat(customHeaderOptions.getInferenceId(),equalTo("test-interface-id"));
        assertThat(customHeaderOptions.getEnableExplanations(),equalTo("test-explanation"));
        assertThat(customHeaderOptions.getTargetVariant(),equalTo("test-target-variant"));
        assertThat(customHeaderOptions.getTargetContainerHostname(),equalTo("test-container-host"));
        assertThat(customHeaderOptions.getTargetModel(),equalTo("test-target-model"));

        final AwsAuthenticationOptions awsAuthenticationOptions =
                httpSinkConfiguration.getAwsAuthenticationOptions();

        assertThat(awsAuthenticationOptions.getAwsRegion(),equalTo(Region.US_EAST_2));
        assertThat(awsAuthenticationOptions.getAwsStsExternalId(),equalTo("test-external-id"));
        assertThat(awsAuthenticationOptions.getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(awsAuthenticationOptions.getAwsStsRoleArn(),equalTo("arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role"));

        final ThresholdOptions thresholdOptions = httpSinkConfiguration.getThresholdOptions();
        assertThat(thresholdOptions.getEventCount(),equalTo(2000));
        assertThat(thresholdOptions.getMaximumSize(),instanceOf(ByteCount.class));
    }
}
