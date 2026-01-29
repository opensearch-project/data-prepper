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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void testGetStsRoleArnReturnsNullWhenNotSet() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}",  // sts_role_arn not set
                "max_retries: 5"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertNull(config.getStsRoleArn());
    }

    @Test
    void testGetStsExternalIdReturnsNullWhenNotSet() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}",  // sts_external_id not set
                "max_retries: 5"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertNull(config.getStsExternalId());
    }

    @Test
    void testAwsRegion_throwsWhenHostIsNull() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"mailto:user@example.com\"", // No host component
                "aws: {}"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertThrows(IllegalArgumentException.class, config::getAwsRegion);
    }

    @Test
    void testAwsRegion_throwsWhenUriIsMalformed() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"https://xray .us-west-2.amazonaws.com\"", // Invalid URI with space
                "aws: {}"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, config::getAwsRegion);
        assertTrue(thrown.getMessage().contains("Failed to parse AWS region from endpoint"));
    }

    @Test
    void testMinimumConfigDefaults() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:      \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(DEFAULT_MAX_RETRIES, config.getMaxRetries());
        assertEquals(DEFAULT_MAX_EVENTS, config.getMaxEvents());
        assertEquals(DEFAULT_BATCH_BYTES, config.getMaxBatchSize());
        assertEquals(DEFAULT_FLUSH_TIMEOUT, config.getFlushTimeoutMillis());

        assertThat(config.getStsRoleArn(), nullValue());
        assertThat(config.getStsExternalId(), nullValue());
        assertThat(config.getStsHeaderOverrides(), nullValue());
    }

    @Test
    void testCustomThresholdAndRetries() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:      \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}",
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
    void testIsAwsConfigValid_returnsTrue_whenPresent() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}"
        );
        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);
        assertTrue(config.isAwsConfigValid());
    }

    @Test
    void testIsAwsConfigValid_returnsFalse_whenMissing() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"" + EXPECTED_ENDPOINT + "\""
        );
        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);
        assertFalse(config.isAwsConfigValid());
    }

    @Test
    void testAwsBlockDeserialization() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:      \"" + EXPECTED_ENDPOINT + "\"",
                "max_retries:   " + DEFAULT_MAX_RETRIES,
                "aws:",
                "  sts_role_arn:    \"" + EXPECTED_ROLE_ARN + "\"",
                "  sts_external_id: \"" + EXPECTED_EXTERNAL_ID + "\""
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals(EXPECTED_ENDPOINT, config.getEndpoint());
        assertEquals(DEFAULT_MAX_RETRIES, config.getMaxRetries());

        assertEquals(EXPECTED_ROLE_ARN, config.getStsRoleArn());
        assertEquals(EXPECTED_EXTERNAL_ID, config.getStsExternalId());
    }

    @Test
    void testAwsSectionPresentButEmpty_doesNotThrow() throws Exception {
        final String yaml = String.join("\n",
                "endpoint:    \"" + EXPECTED_ENDPOINT + "\"",
                "aws: {}",
                "max_retries: " + DEFAULT_MAX_RETRIES
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        // aws block exists, so validateAwsConfig() will not throw
        assertNull(config.getStsRoleArn());
        assertNull(config.getStsExternalId());
    }

    @Test
    void testAwsRegion_parsedFromStandardXrayEndpoint() throws Exception {
        final String yaml = String.join("\n",
                "endpoint: \"https://xray.us-east-1.amazonaws.com\"",
                "aws: {}",
                "max_retries: 5"
        );

        final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);

        assertEquals("https://xray.us-east-1.amazonaws.com", config.getEndpoint());
        assertEquals(5, config.getMaxRetries());

        assertThat(config.getAwsRegion(), equalTo(Region.US_EAST_1));
    }

    @Test
    void testAwsRegion_invalidEndpoint_throwsException() {
        final String yaml = String.join("\n",
                "endpoint: \"https://example.invalid-endpoint\"",
                "aws: {}",
                "max_retries: 5"
        );

        assertThrows(IllegalArgumentException.class, () -> {
            final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);
            config.getAwsRegion();  // triggers parsing
        });
    }

    @Test
    void testAwsRegion_throwsException_onInvalidEndpoint() {
        final String yaml = String.join("\n",
                "endpoint: \"invalid-endpoint\"",
                "aws: {}",
                "max_retries: 5"
        );

        assertThrows(IllegalArgumentException.class, () -> {
            final OtlpSinkConfig config = mapper.readValue(yaml, OtlpSinkConfig.class);
            config.getAwsRegion();  // must trigger parsing logic
        });
    }
}