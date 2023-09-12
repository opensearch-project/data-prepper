/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions.HTTP_BASIC;
import static org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions.UNAUTHENTICATED;

public class PrometheusSinkConfigurationTest {

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        proxy: test-proxy\n" +
            "        http_method: \"POST\"\n" +
            "        auth_type: \"http_basic\"\n" +
            "        authentication:\n" +
            "          http_basic:\n" +
            "            username: \"username\"\n" +
            "            password: \"vip\"\n" +
            "          bearer_token:\n" +
            "            client_id: 0oaafr4j79segrYGC5d7\n" +
            "            client_secret: fFel-3FutCXAOndezEsOVlght6D6DR4OIt7G5D1_oJ6w0wNoaYtgU17JdyXmGf0M\n" +
            "            token_url: https://localhost/oauth2/default/v1/token\n" +
            "            grant_type: client_credentials\n" +
            "            scope: httpSink\n"+
            "        insecure_skip_verify: true\n" +
            "        dlq_file: \"/your/local/dlq-file\"\n" +
            "        dlq:\n" +
            "          s3:\n" +
            "            bucket: dlq.test\n" +
            "            key_path_prefix: \\dlq\"\n" +
            "        ssl_certificate_file: \"/full/path/to/certfile.crt\"\n" +
            "        ssl_key_file: \"/full/path/to/keyfile.key\"\n" +
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        max_retries: 5\n" +
            "        custom_header:\n" +
            "          X-Amzn-SageMaker-Custom-Attributes: [\"test-attribute\"]\n" +
            "          X-Amzn-SageMaker-Target-Model: [\"test-target-model\"]\n" +
            "          X-Amzn-SageMaker-Target-Variant: [\"test-target-variant\"]\n" +
            "          X-Amzn-SageMaker-Target-Container-Hostname: [\"test-container-host\"]\n" +
            "          X-Amzn-SageMaker-Inference-Id: [\"test-interface-id\"]\n" +
            "          X-Amzn-SageMaker-Enable-Explanations: [\"test-explanation\"]";

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    @Test
    void default_proxy_test() {
        assertNull(new PrometheusSinkConfiguration().getProxy());
    }

    @Test
    void default_http_method_test() {
        assertThat(new PrometheusSinkConfiguration().getHttpMethod(), CoreMatchers.equalTo(HTTPMethodOptions.POST));
    }

    @Test
    void default_auth_type_test() {
        assertThat(new PrometheusSinkConfiguration().getAuthType(), equalTo(UNAUTHENTICATED));
    }

    @Test
    void get_url_test() {
        assertThat(new PrometheusSinkConfiguration().getUrl(), equalTo(null));
    }

    @Test
    void get_authentication_test() {
        assertNull(new PrometheusSinkConfiguration().getAuthentication());
    }

    @Test
    void default_ssl_test() {
        assertThat(new PrometheusSinkConfiguration().isInsecureSkipVerify(), equalTo(false));
    }


    @Test
    void get_ssl_certificate_file_test() {
        assertNull(new PrometheusSinkConfiguration().getSslCertificateFile());
    }

    @Test
    void get_ssl_key_file_test() {
        assertNull(new PrometheusSinkConfiguration().getSslKeyFile());
    }

    @Test
    void default_max_upload_retries_test() {
        assertThat(new PrometheusSinkConfiguration().getMaxUploadRetries(), equalTo(5));
    }

    @Test
    void get_aws_authentication_options_test() {
        assertNull(new PrometheusSinkConfiguration().getAwsAuthenticationOptions());
    }

    @Test
    void get_custom_header_options_test() {
        assertNull(new PrometheusSinkConfiguration().getCustomHeaderOptions());
    }

    @Test
    void get_http_retry_interval_test() {
        assertThat(new PrometheusSinkConfiguration().getHttpRetryInterval(),equalTo(PrometheusSinkConfiguration.DEFAULT_HTTP_RETRY_INTERVAL));
    }
    @Test
    void get_acm_private_key_password_test() {assertNull(new PrometheusSinkConfiguration().getAcmPrivateKeyPassword());}

    @Test
    void get_is_ssl_cert_and_key_file_in_s3_test() {assertThat(new PrometheusSinkConfiguration().isSslCertAndKeyFileInS3(), equalTo(false));}

    @Test
    void get_acm_cert_issue_time_out_millis_test() {assertThat(new PrometheusSinkConfiguration().getAcmCertIssueTimeOutMillis(), equalTo(new Long(PrometheusSinkConfiguration.DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS)));}

    @Test
    void http_sink_pipeline_test_with_provided_config_options() throws JsonProcessingException {
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML, PrometheusSinkConfiguration.class);

        assertThat(prometheusSinkConfiguration.getUrl(),equalTo("http://localhost:8080/test"));
        assertThat(prometheusSinkConfiguration.getHttpMethod(),equalTo(HTTPMethodOptions.POST));
        assertThat(prometheusSinkConfiguration.getAuthType(),equalTo(HTTP_BASIC));
        assertThat(prometheusSinkConfiguration.getMaxUploadRetries(),equalTo(5));
        assertThat(prometheusSinkConfiguration.getProxy(),equalTo("test-proxy"));
        assertThat(prometheusSinkConfiguration.getSslCertificateFile(),equalTo("/full/path/to/certfile.crt"));
        assertThat(prometheusSinkConfiguration.getSslKeyFile(),equalTo("/full/path/to/keyfile.key"));
        assertThat(prometheusSinkConfiguration.getDlqFile(),equalTo("/your/local/dlq-file"));

        final Map<String, List<String>> customHeaderOptions = prometheusSinkConfiguration.getCustomHeaderOptions();
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Custom-Attributes"),equalTo(List.of("test-attribute")));
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Inference-Id"),equalTo(List.of("test-interface-id")));
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Enable-Explanations"),equalTo(List.of("test-explanation")));
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Target-Variant"),equalTo(List.of("test-target-variant")));
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Target-Container-Hostname"),equalTo(List.of("test-container-host")));
        assertThat(customHeaderOptions.get("X-Amzn-SageMaker-Target-Model"),equalTo(List.of("test-target-model")));

        final AwsAuthenticationOptions awsAuthenticationOptions =
                prometheusSinkConfiguration.getAwsAuthenticationOptions();

        assertThat(awsAuthenticationOptions.getAwsRegion(),equalTo(Region.US_EAST_2));
        assertThat(awsAuthenticationOptions.getAwsStsExternalId(),equalTo("test-external-id"));
        assertThat(awsAuthenticationOptions.getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(awsAuthenticationOptions.getAwsStsRoleArn(),equalTo("arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role"));

        Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("bucket", "dlq.test");
        pluginSettings.put("key_path_prefix", "dlq");
        final PluginModel pluginModel = new PluginModel("s3", pluginSettings);
        assertThat(prometheusSinkConfiguration.getDlq(), instanceOf(PluginModel.class));
    }

    @Test
    public void validate_and_initialize_cert_and_key_file_in_s3_test() throws JsonProcessingException {
        final String SINK_YAML =
                "        url: \"https://httpbin.org/post\"\n" +
                        "        http_method: \"POST\"\n" +
                        "        auth_type: \"http-basic\"\n" +
                        "        authentication:\n" +
                        "          http_basic:\n" +
                        "            username: \"username\"\n" +
                        "            password: \"vip\"\n" +
                        "        insecure_skip_verify: false\n" +
                        "        use_acm_cert_for_ssl: false\n"+
                        "        acm_certificate_arn: acm_cert\n" +
                        "        ssl_certificate_file: \"/full/path/to/certfile.crt\"\n" +
                        "        ssl_key_file: \"/full/path/to/keyfile.key\"\n" +
                        "        max_retries: 5\n";
        final PrometheusSinkConfiguration httpSinkConfiguration = objectMapper.readValue(SINK_YAML, PrometheusSinkConfiguration.class);
        httpSinkConfiguration.validateAndInitializeCertAndKeyFileInS3();
    }

    @Test
    public void is_valid_aws_url_positive_test() throws JsonProcessingException {

        final String SINK_YAML =
                "        url: \"https://eihycslfo6g2hwrrytyckjkkok.lambda-url.us-east-2.on.aws/\"\n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML, PrometheusSinkConfiguration.class);

        assertTrue(prometheusSinkConfiguration.isValidAWSUrl());
    }

    @Test
    public void is_valid_aws_url_negative_test() throws JsonProcessingException {

        final String SINK_YAML =
                "        url: \"http://localhost:8080/post\"\n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML, PrometheusSinkConfiguration.class);

        assertFalse(prometheusSinkConfiguration.isValidAWSUrl());
    }
}
