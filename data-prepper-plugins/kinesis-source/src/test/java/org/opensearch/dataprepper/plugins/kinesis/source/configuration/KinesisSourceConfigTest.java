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
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private static final String PIPELINE_CONFIG_STREAM_ARN_ENABLED = "pipeline_with_stream_arn_config.yaml";
    private static final String PIPELINE_CONFIG_STREAM_ARN_CONSUMER_ARN_ENABLED = "pipeline_with_stream_arn_consumer_arn_config.yaml";
    private static final String PIPELINE_CONFIG_WITH_METRICS_ENABLED = "pipeline_with_metrics_enabled.yaml";
    private static final String PIPELINE_CONFIG_WITH_INITIAL_POSITION_AT_TIMESTAMP = "pipeline_with_initial_position_at_timestamp_config.yaml";
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
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
        assertTrue(kinesisSourceConfig.isAcknowledgments());
        assertTrue(kinesisSourceConfig.isKclMetricsEnabled());
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
            assertNull(kinesisStreamConfig.getStreamArn());
            assertNull(kinesisStreamConfig.getConsumerArn());
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
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertTrue(kinesisSourceConfig.isKclMetricsEnabled());
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
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
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
            assertNull(kinesisStreamConfig.getStreamArn());
            assertNull(kinesisStreamConfig.getConsumerArn());
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.TRIM_HORIZON);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), expectedCheckpointIntervals.get(kinesisStreamConfig.getName()));
            assertEquals(kinesisStreamConfig.getCompression(), CompressionOption.GZIP);
        }
    }

    @Test
    @Tag(PIPELINE_CONFIG_STREAM_ARN_ENABLED)
    void testSourceConfigWithStreamArn() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertEquals(KinesisSourceConfig.DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT, kinesisSourceConfig.getShardAcknowledgmentTimeout());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsExternalId());
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsHeaderOverrides());
        assertNotNull(kinesisSourceConfig.getCodec());
        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();

        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getStreamArn().contains("arn:aws:kinesis:us-east-1:123456789012:stream/stream"));
            assertNotNull(kinesisStreamConfig.getStreamArn());
            assertNull(kinesisStreamConfig.getConsumerArn());
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), MINIMAL_CHECKPOINT_INTERVAL);
        }
    }

    @Test
    @Tag(PIPELINE_CONFIG_STREAM_ARN_CONSUMER_ARN_ENABLED)
    void testSourceConfigWithStreamArnConsumerArn() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertEquals(KinesisSourceConfig.DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT, kinesisSourceConfig.getShardAcknowledgmentTimeout());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsExternalId());
        assertNull(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsHeaderOverrides());
        assertNotNull(kinesisSourceConfig.getCodec());
        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();

        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            String streamArn = kinesisStreamConfig.getStreamArn();
            assertTrue(streamArn.contains("arn:aws:kinesis:us-east-1:123456789012:stream/stream"));
            assertNotNull(streamArn);
            assertNotNull(kinesisStreamConfig.getConsumerArn());
            assertTrue(kinesisStreamConfig.getConsumerArn().contains(streamArn+"/consumer/consumer-1:1"));
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointInterval(), MINIMAL_CHECKPOINT_INTERVAL);
        }
    }

    @Test
    @Tag(PIPELINE_CONFIG_WITH_METRICS_ENABLED)
    void testSourceConfigWithMetricsEnabled() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertTrue(kinesisSourceConfig.isKclMetricsEnabled());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertFalse(kinesisSourceConfig.isAcknowledgments());
        assertThat(kinesisSourceConfig.getAwsAuthenticationConfig(), notNullValue());
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion(), Region.US_EAST_1);
        assertEquals(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn(), "arn:aws:iam::123456789012:role/OSI-PipelineRole");

        List<KinesisStreamConfig> streamConfigs = kinesisSourceConfig.getStreams();
        assertNotNull(kinesisSourceConfig.getCodec());
        assertEquals(streamConfigs.size(), 3);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
        }
    }
    @Tag(PIPELINE_CONFIG_WITH_INITIAL_POSITION_AT_TIMESTAMP)
    void testSourceConfigWithInitialPositionAtTimestamp() {

        assertThat(kinesisSourceConfig, notNullValue());
        assertEquals(KinesisSourceConfig.DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, kinesisSourceConfig.getNumberOfRecordsToAccumulate());
        assertEquals(KinesisSourceConfig.DEFAULT_TIME_OUT_IN_MILLIS, kinesisSourceConfig.getBufferTimeout());
        assertEquals(KinesisSourceConfig.DEFAULT_MAX_INITIALIZATION_ATTEMPTS, kinesisSourceConfig.getMaxInitializationAttempts());
        assertEquals(KinesisSourceConfig.DEFAULT_INITIALIZATION_BACKOFF_TIME, kinesisSourceConfig.getInitializationBackoffTime());
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

        assertEquals(streamConfigs.size(), 2);

        Map<String, KinesisStreamConfig> streamConfigMap = streamConfigs.stream()
                .collect(Collectors.toMap(KinesisStreamConfig::getName, config -> config));

        assertEquals(2, streamConfigMap.size());

        KinesisStreamConfig stream1 = streamConfigMap.get("stream-1");
        assertNotNull(stream1);
        assertNull(stream1.getStreamArn());
        assertNull(stream1.getConsumerArn());
        assertEquals(InitialPositionInStream.AT_TIMESTAMP, stream1.getInitialPosition());
        assertEquals(Duration.parse("P3DT12H"), stream1.getRange());
        assertNull(stream1.getInitialTimestamp());

        KinesisStreamConfig stream2 = streamConfigMap.get("stream-2");
        assertNotNull(stream2);
        assertNull(stream2.getStreamArn());
        assertNull(stream2.getConsumerArn());
        assertEquals(InitialPositionInStream.AT_TIMESTAMP, stream2.getInitialPosition());
        assertNull(stream2.getRange());
        assertEquals(LocalDateTime.parse("2024-01-15T10:30:00"), stream2.getInitialTimestamp());
    }

}