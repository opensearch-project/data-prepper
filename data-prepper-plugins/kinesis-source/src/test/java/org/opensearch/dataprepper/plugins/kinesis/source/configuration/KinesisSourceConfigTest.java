/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KinesisSourceConfigTest {
    private static final String PIPELINE_CONFIG_WITH_ACKS_ENABLED = "pipeline_with_acks_enabled.yaml";
    private static final String PIPELINE_CONFIG_WITH_POLLING_CONFIG_ENABLED = "pipeline_with_polling_config_enabled.yaml";
    private static final String PIPELINE_CONFIG_CHECKPOINT_ENABLED = "pipeline_with_checkpoint_enabled.yaml";
    private static final Duration MINIMAL_CHECKPOINT_INTERVAL = Duration.ofMillis(2 * 60 * 1000); // 2 minute

    KinesisSourceConfig kinesisSourceConfig;

    ObjectMapper objectMapper;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTags().stream().findFirst().orElse("");
        final File configurationFile = new File(getClass().getClassLoader().getResource(fileName).getFile());
        objectMapper = new ObjectMapper(new YAMLFactory());
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(simpleModule);

        final Map<String, Object> pipelineConfig = objectMapper.readValue(configurationFile, Map.class);
        final Map<String, Object> sourceMap = (Map<String, Object>) pipelineConfig.get("source");
        final Map<String, Object> kinesisConfigMap = (Map<String, Object>) sourceMap.get("kinesis");
        String json = objectMapper.writeValueAsString(kinesisConfigMap);
        final Reader reader = new StringReader(json);
        kinesisSourceConfig = objectMapper.readValue(reader, KinesisSourceConfig.class);

    }

    @Test
    @Tag(PIPELINE_CONFIG_WITH_ACKS_ENABLED)
    void testSourceConfig() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertTrue(kinesisSourceConfig.isAcknowledgments());
        assertEquals(KinesisSourceConfig.DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT, kinesisSourceConfig.getShardAcknowledgmentTimeout());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsExternalId());
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsHeaderOverrides());

        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();
        assertNotNull(kinesisSourceConfig.getCodec());
        assertEquals(kinesisSourceConfig.getConsumerStrategy(), ConsumerStrategy.ENHANCED_FAN_OUT);
        assertNull(kinesisSourceConfig.getPollingConfig());

        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), MINIMAL_CHECKPOINT_INTERVAL);
        }
    }

    @Test
    @Tag(PIPELINE_CONFIG_WITH_POLLING_CONFIG_ENABLED)
    void testSourceConfigWithStreamCodec() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertEquals(KinesisSourceConfig.DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT, kinesisSourceConfig.getShardAcknowledgmentTimeout());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsExternalId());
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsHeaderOverrides());
        assertNotNull(kinesisSourceConfig.getCodec());
        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();
        assertEquals(kinesisSourceConfig.getConsumerStrategy(), ConsumerStrategy.POLLING);
        assertNotNull(kinesisSourceConfig.getPollingConfig());
        assertEquals(kinesisSourceConfig.getPollingConfig().getMaxPollingRecords(), 10);
        assertEquals(kinesisSourceConfig.getPollingConfig().getIdleTimeBetweenReads(), Duration.ofSeconds(10));

        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), MINIMAL_CHECKPOINT_INTERVAL);
        }
    }

    @Test
    @Tag(PIPELINE_CONFIG_CHECKPOINT_ENABLED)
    void testSourceConfigWithInitialPosition() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertEquals(KinesisSourceConfig.DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT, kinesisSourceConfig.getShardAcknowledgmentTimeout());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsExternalId());
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsHeaderOverrides());
        assertNotNull(kinesisSourceConfig.getCodec());
        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();
        assertEquals(kinesisSourceConfig.getConsumerStrategy(), ConsumerStrategy.ENHANCED_FAN_OUT);

        Map<String, Duration> expectedCheckpointIntervals = new HashMap<>();
        expectedCheckpointIntervals.put("stream-1", Duration.ofSeconds(20));
        expectedCheckpointIntervals.put("stream-2", Duration.ofMinutes(15));
        expectedCheckpointIntervals.put("stream-3", Duration.ofHours(2));

        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.TRIM_HORIZON);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), expectedCheckpointIntervals.get(kinesisStreamConfig.getName()));
        }
    }
}
