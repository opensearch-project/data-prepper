/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    @Mock
    private KafkaSource source;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExecutorService executorService;

    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private TopicConfig topicConfig;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    List<TopicConfig> mockList = new ArrayList<TopicConfig>();

    private static final String AUTO_COMMIT = "false";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String AUTO_OFFSET_RESET = "earliest";
    private static final String GROUP_ID = "Kafka-source";
    private static final String TOPIC = "my-topic";
    private static final Integer WORKERS = 3;

    @BeforeEach
    void setUp() throws Exception {
        when(sourceConfig.getTopics()).thenReturn((mockList));
        when(mockList.get(0)).thenReturn(topicConfig);
        when(sourceConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(sourceConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
    }

    @Test
    void test_kafkaSource_start_execution_catch_block() {
        source = new KafkaSource(null, pluginMetrics,pipelineDescription);
        KafkaSource spySource = spy(source);
        Assertions.assertThrows(Exception.class, () -> spySource.start(any()));
    }

    @Test
    void test_kafkaSource_stop_execution() throws Exception {
        List<MultithreadedConsumer> consumers = buildKafkaSourceConsumer();
        source = new KafkaSource(sourceConfig, pluginMetrics,pipelineDescription);
        KafkaSource spySource = spy(source);
        ReflectionTestUtils.setField(spySource, "executorService", executorService);
        doCallRealMethod().when(spySource).stop();
        spySource.stop();
        verify(spySource).stop();
    }

    private List<MultithreadedConsumer> buildKafkaSourceConsumer() {
        List<MultithreadedConsumer> consumers = new ArrayList<>();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, topicConfig,sourceConfig, null, pluginMetrics,null);
        consumers.add(kafkaSourceConsumer);
        return consumers;
    }


    @Test
    void test_kafkaSource_start_execution_string_schemaType() throws Exception {
        List<TopicConfig> topicConfigList = new ArrayList<TopicConfig>();
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig();
        TopicConfig topicConfig = new TopicConfig();
        SchemaConfig schemaConfig = new SchemaConfig();

        topicConfig.setAutoCommitInterval(Duration.ofMillis(1000));
        topicConfig.setAutoOffsetReset(AUTO_OFFSET_RESET);
        topicConfig.setAutoCommit(AUTO_COMMIT);
        topicConfig.setGroupId(GROUP_ID);
        topicConfig.setWorkers(WORKERS);

        sourceConfig.setSchemaConfig(schemaConfig);
        topicConfig.setName(TOPIC);

        topicConfigList.add(topicConfig);
        sourceConfig.setTopics(topicConfigList);
        sourceConfig.setBootStrapServers(Arrays.asList(BOOTSTRAP_SERVERS));

        source = new KafkaSource(sourceConfig, pluginMetrics,pipelineDescription);
        KafkaSource spySource = spy(source);
        doCallRealMethod().when(spySource).start(any());
        spySource.start(any());
        verify(spySource).start(any());
    }

    @Test
    void test_kafkaSource_start_execution_json_schemaType() throws Exception {

        List<TopicConfig> topicConfigList = new ArrayList<TopicConfig>();
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig();
        TopicConfig topicConfig = new TopicConfig();
        SchemaConfig schemaConfig = new SchemaConfig();

        topicConfig.setAutoCommitInterval(Duration.ofMillis(1000));
        topicConfig.setAutoOffsetReset(AUTO_OFFSET_RESET);
        topicConfig.setAutoCommit(AUTO_COMMIT);
        topicConfig.setGroupId(GROUP_ID);
        topicConfig.setWorkers(WORKERS);

        sourceConfig.setSchemaConfig(schemaConfig);
        topicConfig.setName(TOPIC);

        topicConfigList.add(topicConfig);
        sourceConfig.setTopics(topicConfigList);

        sourceConfig.setBootStrapServers(Arrays.asList(BOOTSTRAP_SERVERS));
        source = new KafkaSource(sourceConfig, pluginMetrics,pipelineDescription);
        KafkaSource spySource = spy(source);
        ReflectionTestUtils.setField(spySource, "sourceConfig", sourceConfig);
        doCallRealMethod().when(spySource).start(any());
        spySource.start(any());
        verify(spySource).start(any());
    }
}
