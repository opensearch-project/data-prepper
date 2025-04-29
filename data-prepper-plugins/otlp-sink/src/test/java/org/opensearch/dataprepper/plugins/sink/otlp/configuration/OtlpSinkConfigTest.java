/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OtlpSinkConfigTest {

    private static ObjectMapper mapper;

    private static final String EXPECTED_ENDPOINT = "https://example.com/otlp";
    private static final int DEFAULT_MAX_RETRIES = 5;

    private static final int CUSTOM_MAX_EVENTS = 100;
    private static final String CUSTOM_BATCH_SIZE = "2mb";
    private static final long CUSTOM_BATCH_BYTES = ByteCount.parse(CUSTOM_BATCH_SIZE).getBytes();
    private static final long CUSTOM_FLUSH_TIMEOUT = 500L;

    private static final int DEFAULT_MAX_EVENTS = 512;
    private static final long DEFAULT_BATCH_BYTES = ByteCount.parse("1mb").getBytes();
    private static final long DEFAULT_FLUSH_TIMEOUT = 200L;

    private static final String EXPECTED_REGION = "us-west-2";
    private static final String EXPECTED_ROLE_ARN = "arn:aws:iam::123456789012:role/OtlpRole";
    private static final String EXPECTED_EXTERNAL_ID = "my-ext-id";

    @BeforeAll
    static void setupMapper() {
        mapper = new ObjectMapper(new YAMLFactory())
                .findAndRegisterModules();  // for default Duration support

        // custom deserializer for ByteCount strings like "2mb"
        final SimpleModule byteCountModule = new SimpleModule();
        byteCountModule.addDeserializer(ByteCount.class, new JsonDeserializer<>() {
            @Override
            public ByteCount deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
                return ByteCount.parse(p.getValueAsString());
            }
        });
        mapper.registerModule(byteCountModule);

        // custom deserializer for Duration strings like "500ms"
        final SimpleModule durationModule = new SimpleModule();
        durationModule.addDeserializer(Duration.class, new JsonDeserializer<>() {
            @Override
            public Duration deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
                final String text = p.getValueAsString();
                if (text.endsWith("ms")) {
                    final long ms = Long.parseLong(text.substring(0, text.length() - 2));
                    return Duration.ofMillis(ms);
                }
                return Duration.parse(text);
            }
        });
        mapper.registerModule(durationModule);
    }

    @Test
    void testMinimumConfigDefaults() throws Exception {
        final String yaml = "endpoint: \"" + EXPECTED_ENDPOINT + "\"";

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(DEFAULT_MAX_RETRIES, config.getMaxRetries());
        assertEquals(DEFAULT_MAX_EVENTS, config.getMaxEvents());
        assertEquals(DEFAULT_BATCH_BYTES, config.getMaxBatchSize());
        assertEquals(DEFAULT_FLUSH_TIMEOUT, config.getFlushTimeoutMillis());

        assertThat(config.getAwsRegion(), nullValue());
        assertThat(config.getStsRoleArn(), nullValue());
        assertThat(config.getStsExternalId(), nullValue());
    }

    @Test
    void testCustomThresholdAndRetries() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:      \"" + EXPECTED_ENDPOINT + "\"",
                "max_retries:   3",
                "threshold:",
                "  max_events:     " + CUSTOM_MAX_EVENTS,
                "  max_batch_size: \"" + CUSTOM_BATCH_SIZE + "\"",
                "  flush_timeout:  \"" + CUSTOM_FLUSH_TIMEOUT + "ms\""
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(3, config.getMaxRetries());
        assertEquals(CUSTOM_MAX_EVENTS, config.getMaxEvents());
        assertEquals(CUSTOM_BATCH_BYTES, config.getMaxBatchSize());
        assertEquals(CUSTOM_FLUSH_TIMEOUT, config.getFlushTimeoutMillis());
    }

    @Test
    void testAwsBlockDeserialization() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:      \"" + EXPECTED_ENDPOINT + "\"",
                "max_retries:   " + DEFAULT_MAX_RETRIES,
                "aws:",
                "  region:          \"" + EXPECTED_REGION + "\"",
                "  sts_role_arn:    \"" + EXPECTED_ROLE_ARN + "\"",
                "  sts_external_id: \"" + EXPECTED_EXTERNAL_ID + "\""
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(DEFAULT_MAX_RETRIES, config.getMaxRetries());

        assertEquals(Region.of(EXPECTED_REGION), config.getAwsRegion());
        assertEquals(EXPECTED_ROLE_ARN, config.getStsRoleArn());
        assertEquals(EXPECTED_EXTERNAL_ID, config.getStsExternalId());
    }

    @Test
    void testAwsSectionMissing_staysNull() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:    \"" + EXPECTED_ENDPOINT + "\"",
                "max_retries: " + DEFAULT_MAX_RETRIES
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(DEFAULT_MAX_RETRIES, config.getMaxRetries());

        assertThat(config.getAwsRegion(), nullValue());
        assertThat(config.getStsRoleArn(), nullValue());
        assertThat(config.getStsExternalId(), nullValue());
    }
}