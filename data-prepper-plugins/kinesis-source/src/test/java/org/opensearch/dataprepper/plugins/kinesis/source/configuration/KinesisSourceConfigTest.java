package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
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
    private static final String SIMPLE_PIPELINE_CONFIG = "simple-pipeline.yaml";
    private static final String SIMPLE_PIPELINE_CONFIG_2 = "simple-pipeline-2.yaml";
    private static final int MINIMAL_CHECKPOINT_INTERVAL_MILLIS = 2 * 60 * 1000; // 2 minute
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;

    KinesisSourceConfig kinesisSourceConfig;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTags().stream().findFirst().orElse("");
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(fileName).getFile());
        Object data = yaml.load(fileReader);
        ObjectMapper mapper = new ObjectMapper();
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("kinesis-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kinesisConfigMap = (Map<String, Object>) sourceMap.get("kinesis");
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kinesisConfigMap);
            Reader reader = new StringReader(json);
            kinesisSourceConfig = mapper.readValue(reader, KinesisSourceConfig.class);
        }
    }

    @Test
    @Tag(SIMPLE_PIPELINE_CONFIG)
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
        assertNull(kinesisSourceConfig.getCodec());
        assertEquals(kinesisSourceConfig.getConsumerStrategy(), ConsumerStrategy.ENHANCED_FAN_OUT);
        assertNull(kinesisSourceConfig.getPollingConfig());

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
            assertTrue(kinesisStreamConfig.getArn().contains("123456789012:stream/stream"));
            assertFalse(kinesisStreamConfig.isEnableCheckPoint());
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointIntervalInMilliseconds(), MINIMAL_CHECKPOINT_INTERVAL_MILLIS);
        }
    }

    @Test
    @Tag(SIMPLE_PIPELINE_CONFIG_2)
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
        assertEquals(kinesisSourceConfig.getPollingConfig().getIdleTimeBetweenReadsInMillis(), 10);

        for (KinesisStreamConfig kinesisStreamConfig: streamConfigs) {
            assertTrue(kinesisStreamConfig.getName().contains("stream"));
            assertTrue(kinesisStreamConfig.getArn().contains("123456789012:stream/stream"));
            assertFalse(kinesisStreamConfig.isEnableCheckPoint());
            assertEquals(kinesisStreamConfig.getInitialPosition(), InitialPositionInStream.LATEST);
            assertEquals(kinesisStreamConfig.getCheckPointIntervalInMilliseconds(), MINIMAL_CHECKPOINT_INTERVAL_MILLIS);
        }
    }
}
