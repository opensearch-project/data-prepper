/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.time.Duration;

import static com.linecorp.armeria.common.MediaTypeNames.X_PROTOBUF;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PrometheusSinkConfigurationTest {

    private static final String SINK_YAML =
            " url: \"http://localhost:8080/test\"\n" +
                    " encoding: \"snappy\" \n" +
                    " remote_write_version: \"0.1.0\" \n" +
                    " content_type: \"application/x-protobuf\" \n" +
                    " sanitize_names: false \n" +
                    " aws:\n" +
                    "          region: \"us-east-2\"\n" +
                    "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
                    "          sts_external_id: \"test-external-id\"\n" +
                    "          sts_header_overrides: {\"test\": test }\n" +
                    " connection_timeout: PT45S\n" +
                    " idle_timeout: PT45S\n" +
                    " request_timeout: PT45S\n" +
                    " max_retries: 5\n";

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void prometheus_sink_test_default_config_options() throws JsonProcessingException {
        final PrometheusSinkConfiguration prometheusSinkConfiguration = new PrometheusSinkConfiguration();

        assertThat(prometheusSinkConfiguration.getUrl(), equalTo(null));
        assertThat(prometheusSinkConfiguration.getMaxRetries(), equalTo(PrometheusSinkConfiguration.DEFAULT_MAX_RETRIES));
        assertThat(prometheusSinkConfiguration.getConnectionTimeout(), equalTo(Duration.ofSeconds(60)));
        assertThat(prometheusSinkConfiguration.getIdleTimeout(), equalTo(Duration.ofSeconds(60)));
        assertThat(prometheusSinkConfiguration.getRequestTimeout(), equalTo(Duration.ofSeconds(60)));
        assertThat(prometheusSinkConfiguration.getEncoding(), equalTo(CompressionOption.SNAPPY));
        assertThat(prometheusSinkConfiguration.getContentType(), equalTo(X_PROTOBUF));
        assertThat(prometheusSinkConfiguration.getSanitizeNames(), equalTo(true));
        assertThat(prometheusSinkConfiguration.getRemoteWriteVersion(), equalTo(PrometheusSinkConfiguration.DEFAULT_REMOTE_WRITE_VERSION));
    }

    @Test
    void prometheus_sink_test_with_provided_config_options() throws JsonProcessingException {
        objectMapper.registerModule(new JavaTimeModule());
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML, PrometheusSinkConfiguration.class);

        assertThat(prometheusSinkConfiguration.getUrl(), equalTo("http://localhost:8080/test"));
        assertThat(prometheusSinkConfiguration.getMaxRetries(), equalTo(5));
        assertThat(prometheusSinkConfiguration.getConnectionTimeout(), equalTo(Duration.ofSeconds(45)));
        assertThat(prometheusSinkConfiguration.getIdleTimeout(), equalTo(Duration.ofSeconds(45)));
        assertThat(prometheusSinkConfiguration.getRequestTimeout(), equalTo(Duration.ofSeconds(45)));
        assertThat(prometheusSinkConfiguration.getEncoding(), equalTo(CompressionOption.SNAPPY));
        assertThat(prometheusSinkConfiguration.getContentType(), equalTo(X_PROTOBUF));
        assertThat(prometheusSinkConfiguration.getSanitizeNames(), equalTo(false));
        assertThat(prometheusSinkConfiguration.getRemoteWriteVersion(), equalTo(PrometheusSinkConfiguration.DEFAULT_REMOTE_WRITE_VERSION));
    }

    @Test
    void prometheus_sink_config_test_with_invalid_encoding() throws JsonProcessingException {
        final String INVALID_SINK_YAML =
            " url: \"https://localhost:8080/test\"\n" +
                    " encoding: \"not_snappy\" \n" +
                    " remote_write_version: \"0.1.0\" \n" +
                    " content_type: \"application/x-protobuf\" \n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(INVALID_SINK_YAML, PrometheusSinkConfiguration.class);
        assertFalse(prometheusSinkConfiguration.isValidConfig());
    }

    @Test
    void prometheus_sink_config_test_with_invalid_remote_write_version() throws JsonProcessingException {
        final String INVALID_SINK_YAML =
            " url: \"https://localhost:8080/test\"\n" +
                    " encoding: \"snappy\" \n" +
                    " remote_write_version: \"1.1.0\" \n" +
                    " content_type: \"application/x-protobuf\" \n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(INVALID_SINK_YAML, PrometheusSinkConfiguration.class);
        assertFalse(prometheusSinkConfiguration.isValidConfig());
    }

    @Test
    void prometheus_sink_config_test_with_invalid_content_type() throws JsonProcessingException {
        final String INVALID_SINK_YAML =
            " url: \"https://localhost:8080/test\"\n" +
                    " encoding: \"snappy\" \n" +
                    " remote_write_version: \"0.1.0\" \n" +
                    " content_type: \"application/json\" \n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(INVALID_SINK_YAML, PrometheusSinkConfiguration.class);
        assertFalse(prometheusSinkConfiguration.isValidConfig());
    }

    @Test
    void prometheus_sink_config_test_with_invalid_url() throws JsonProcessingException {
        final String INVALID_SINK_YAML =
            " url: \"http://localhost:8080/test\"\n" +
                    " encoding: \"snappy\" \n" +
                    " remote_write_version: \"0.1.0\" \n" +
                    " content_type: \"application/x-protobuf\" \n";
        final PrometheusSinkConfiguration prometheusSinkConfiguration = objectMapper.readValue(INVALID_SINK_YAML, PrometheusSinkConfiguration.class);
        assertFalse(prometheusSinkConfiguration.isValidConfig());
    }
}
