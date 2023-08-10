/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopicConfigTest {

    @Mock
    TopicConfig topicsConfig;

    private static final String YAML_FILE_WITH_CONSUMER_CONFIG = "sample-pipelines.yaml";

    private static final String YAML_FILE_WITH_MISSING_CONSUMER_CONFIG = "sample-pipelines-1.yaml";

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTags().stream().findFirst().orElse("");
        topicsConfig = new TopicConfig();
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(fileName).getFile());
        Object data = yaml.load(fileReader);
        ObjectMapper mapper = new ObjectMapper();
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            KafkaSourceConfig kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            List<TopicConfig> topicsConfigList = kafkaSourceConfig.getTopics();
            topicsConfig = topicsConfigList.get(0);
        }
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void test_topicsConfig_not_null() {
        assertThat(topicsConfig, notNullValue());
    }

    @Test
    @Tag(YAML_FILE_WITH_MISSING_CONSUMER_CONFIG)
    void testConfigValues_default() {
        assertEquals("my-topic-2", topicsConfig.getName());
        assertEquals("my-test-group", topicsConfig.getGroupId());
        assertEquals("kafka-consumer-group-2", topicsConfig.getGroupName());
        assertEquals(false, topicsConfig.getAutoCommit());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getCommitInterval());
        assertEquals(Duration.ofSeconds(45), topicsConfig.getSessionTimeOut());
        assertEquals("latest", topicsConfig.getAutoOffsetReset());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getThreadWaitingTime());
        assertEquals(Duration.ofSeconds(4), topicsConfig.getMaxRecordFetchTime());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getBufferDefaultTimeout());
        assertEquals(52428800L, topicsConfig.getFetchMaxBytes().longValue());
        assertEquals(500L, topicsConfig.getFetchMaxWait().longValue());
        assertEquals(1L, topicsConfig.getFetchMinBytes().longValue());
        assertEquals(Duration.ofSeconds(10), topicsConfig.getRetryBackoff());
        assertEquals(Duration.ofSeconds(300000), topicsConfig.getMaxPollInterval());
        assertEquals(500L, topicsConfig.getConsumerMaxPollRecords().longValue());
        assertEquals(2, topicsConfig.getWorkers().intValue());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getHeartBeatInterval());
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void testConfigValues_from_yaml() {

        assertEquals("my-topic-1", topicsConfig.getName());
        assertEquals("my-test-group", topicsConfig.getGroupId());
        assertEquals(null, topicsConfig.getGroupName());
        assertEquals(false, topicsConfig.getAutoCommit());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getCommitInterval());
        assertEquals(Duration.ofSeconds(45), topicsConfig.getSessionTimeOut());
        assertEquals("earliest", topicsConfig.getAutoOffsetReset());
        assertEquals(Duration.ofSeconds(1), topicsConfig.getThreadWaitingTime());
        assertEquals(Duration.ofSeconds(4), topicsConfig.getMaxRecordFetchTime());
        assertEquals(Duration.ofSeconds(5), topicsConfig.getBufferDefaultTimeout());
        assertEquals(52428800L, topicsConfig.getFetchMaxBytes().longValue());
        assertEquals(500L, topicsConfig.getFetchMaxWait().longValue());
        assertEquals(1L, topicsConfig.getFetchMinBytes().longValue());
        assertEquals(Duration.ofSeconds(100), topicsConfig.getRetryBackoff());
        assertEquals(Duration.ofSeconds(300000), topicsConfig.getMaxPollInterval());
        assertEquals(500L, topicsConfig.getConsumerMaxPollRecords().longValue());
        assertEquals(5, topicsConfig.getWorkers().intValue());
        assertEquals(Duration.ofSeconds(3), topicsConfig.getHeartBeatInterval());
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void testConfigValues_from_yaml_not_null() {

        assertNotNull(topicsConfig.getName());
        assertNotNull(topicsConfig.getGroupId());
        assertNotNull(topicsConfig.getAutoCommit());
        assertNotNull(topicsConfig.getCommitInterval());
        assertNotNull(topicsConfig.getSessionTimeOut());
        assertNotNull(topicsConfig.getAutoOffsetReset());
        assertNotNull(topicsConfig.getThreadWaitingTime());
        assertNotNull(topicsConfig.getMaxRecordFetchTime());
        assertNotNull(topicsConfig.getBufferDefaultTimeout());
        assertNotNull(topicsConfig.getFetchMaxBytes());
        assertNotNull(topicsConfig.getFetchMaxWait());
        assertNotNull(topicsConfig.getFetchMinBytes());
        assertNotNull(topicsConfig.getRetryBackoff());
        assertNotNull(topicsConfig.getMaxPollInterval());
        assertNotNull(topicsConfig.getConsumerMaxPollRecords());
        assertNotNull(topicsConfig.getWorkers());
        assertNotNull(topicsConfig.getHeartBeatInterval());
    }

}
