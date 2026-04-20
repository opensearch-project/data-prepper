/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.types.ByteCount;
import software.amazon.awssdk.regions.Region;
import org.opensearch.dataprepper.aws.api.AwsConfig;

import java.util.HashMap;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HttpSinkConfigurationTest {

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        threshold:\n" +
            "          max_events: 2000\n" +
            "          max_request_size: 2mb\n" +
            "        max_retries: 5\n";

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));




    @Test
    void default_codec_test() {
        assertNull(new HttpSinkConfiguration().getCodec());
    }

    @Test
    void get_url_test() {
        assertThat(new HttpSinkConfiguration().getUrl(), equalTo(null));
    }

    @Test
    void get_threshold_options_test() {
        assertThat(new HttpSinkConfiguration().getThresholdOptions(), instanceOf(ThresholdOptions.class));
    }

    @Test
    void default_max_upload_retries_test() {
        assertThat(new HttpSinkConfiguration().getMaxUploadRetries(), equalTo(HttpSinkConfiguration.DEFAULT_UPLOAD_RETRIES));
    }

    @Test
    void get_aws_authentication_options_test() {
        assertNull(new HttpSinkConfiguration().getAwsConfig());
    }

    @Test
    void get_http_retry_interval_test() {
        assertThat(new HttpSinkConfiguration().getHttpRetryInterval(),equalTo(HttpSinkConfiguration.DEFAULT_HTTP_RETRY_INTERVAL));
    }

    @Test
    void http_sink_pipeline_test_with_provided_config_options() throws JsonProcessingException {
        final HttpSinkConfiguration httpSinkConfiguration = objectMapper.readValue(SINK_YAML, HttpSinkConfiguration.class);

        assertThat(httpSinkConfiguration.getUrl(),equalTo("http://localhost:8080/test"));
        assertThat(httpSinkConfiguration.getMaxUploadRetries(),equalTo(5));

        final AwsConfig awsConfig =
                httpSinkConfiguration.getAwsConfig();

        assertThat(awsConfig.getAwsRegion(),equalTo(Region.US_EAST_2));
        assertThat(awsConfig.getAwsStsExternalId(),equalTo("test-external-id"));
        assertThat(awsConfig.getAwsStsHeaderOverrides().get("test"),equalTo("test"));
        assertThat(awsConfig.getAwsStsRoleArn(),equalTo("arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role"));

        final ThresholdOptions thresholdOptions = httpSinkConfiguration.getThresholdOptions();
        assertThat(thresholdOptions.getMaxEvents(),equalTo(2000));
        assertThat(thresholdOptions.getMaxRequestSize(),instanceOf(ByteCount.class));

        Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("bucket", "dlq.test");
        pluginSettings.put("key_path_prefix", "dlq");
        final PluginModel pluginModel = new PluginModel("s3", pluginSettings);
    }

}
