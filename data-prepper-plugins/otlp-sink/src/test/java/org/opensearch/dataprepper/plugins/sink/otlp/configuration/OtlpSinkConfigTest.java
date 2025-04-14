/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

class OtlpSinkConfigTest {

    private static final String EXPECTED_ENDPOINT = "https://example.com/otlp";
    private static final int EXPECTED_BATCH_SIZE = 128;
    private static final int EXPECTED_MAX_RETRIES = 4;
    private static final String EXPECTED_REGION = "us-west-2";
    private static final String EXPECTED_ROLE_ARN = "arn:aws:iam::123456789012:role/OtlpRole";
    private static final String EXPECTED_EXTERNAL_ID = "my-ext-id";

    @Test
    void testDeserializationFromYaml() throws Exception {
        final String yaml =
                "endpoint: \"" + EXPECTED_ENDPOINT + "\"\n" +
                        "batch_size: " + EXPECTED_BATCH_SIZE + "\n" +
                        "max_retries: " + EXPECTED_MAX_RETRIES + "\n" +
                        "aws:\n" +
                        "  region: \"" + EXPECTED_REGION + "\"\n" +
                        "  sts_role_arn: \"" + EXPECTED_ROLE_ARN + "\"\n" +
                        "  sts_external_id: \"" + EXPECTED_EXTERNAL_ID + "\"\n";

        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        final OtlpSinkConfig config = objectMapper.readValue(yaml, OtlpSinkConfig.class);

        assertThat(config.getEndpoint(), equalTo(EXPECTED_ENDPOINT));
        assertThat(config.getBatchSize(), equalTo(EXPECTED_BATCH_SIZE));
        assertThat(config.getMaxRetries(), equalTo(EXPECTED_MAX_RETRIES));

        assertThat(config.getAwsRegion().toString(), equalTo(EXPECTED_REGION));
        assertThat(config.getStsRoleArn(), equalTo(EXPECTED_ROLE_ARN));
        assertThat(config.getStsExternalId(), equalTo(EXPECTED_EXTERNAL_ID));
    }

    @Test
    void testDeserializationFromYaml_withNullAwsValues() throws Exception {
        final String yaml = "{}";
        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        final OtlpSinkConfig config = objectMapper.readValue(yaml, OtlpSinkConfig.class);

        assertThat(config.getAwsRegion(), nullValue());
        assertThat(config.getStsRoleArn(), nullValue());
        assertThat(config.getStsExternalId(), nullValue());
    }
}