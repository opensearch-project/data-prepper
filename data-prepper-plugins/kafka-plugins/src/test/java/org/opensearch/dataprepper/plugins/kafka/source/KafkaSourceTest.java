/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

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

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.ConsumerConfigs;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doCallRealMethod;

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
	private TopicsConfig topicsConfig;
	
	@Mock
	List<TopicsConfig> mockList = new ArrayList<TopicsConfig>();
	
	@Mock
	private TopicConfig topicConfig;
	
	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(topicsConfig.getTopic()).thenReturn(topicConfig);
		
		when(topicConfig.getSchemaConfig()).thenReturn(schemaConfig);
		
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(topicConfig.getConsumerGroupConfig().getWorkers()).thenReturn(1);
		when(topicConfig.getConsumerGroupConfig().getGroupName()).thenReturn("DPKafkaProj");
		when(topicConfig.getConsumerGroupConfig().getAutoOffsetReset()).thenReturn("earliest");
		when(sourceConfig.getBootStrapServers()).thenReturn(Arrays.asList("localhost:9092"));
		when(topicConfig.getConsumerGroupConfig().getGroupId()).thenReturn("DPKafkaProj");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(topicConfig.getName()).thenReturn(("my-topic"));

	}

	@Test
	void test_kafkaSource_start_execution_catch_block() {
		source = new KafkaSource(null, pluginMetrics);
		KafkaSource spySource = spy(source);
		Assertions.assertThrows(Exception.class, () -> spySource.start(any()));
	}

	@Test
	void test_kafkaSource_stop_execution() throws Exception {
		List<MultithreadedConsumer> consumers = buildKafkaSourceConsumer();
		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		ReflectionTestUtils.setField(spySource, "executorService", executorService);
		doCallRealMethod().when(spySource).stop();
		spySource.stop();
		verify(spySource).stop();
	}

	private List<MultithreadedConsumer> buildKafkaSourceConsumer() {
		List<MultithreadedConsumer> consumers = new ArrayList<>();
		Properties prop = new Properties();
		prop.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(
				topicConfig.getConsumerGroupConfig().getGroupId(), 
				topicConfig.getConsumerGroupConfig().getGroupId(),
				prop, topicConfig, null, pluginMetrics);
		consumers.add(kafkaSourceConsumer);
		return consumers;
	}


	@Test
	void test_kafkaSource_start_execution_string_schemaType() throws Exception {
		List<TopicsConfig> list = new ArrayList<TopicsConfig>();
		TopicConfig topicConfig = new TopicConfig();
		KafkaSourceConfig sourceConfig = new KafkaSourceConfig();
		TopicsConfig topicsConfig = new TopicsConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		SchemaConfig schemaConfig = new SchemaConfig();
		schemaConfig.setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
		schemaConfig.setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
		schemaConfig.setSchemaType("plaintext");
		consumerConfigs.setAutoCommitInterval(Duration.ofMillis(1000));
		consumerConfigs.setAutoOffsetReset("earliest");
		consumerConfigs.setAutoCommit("false");
		consumerConfigs.setGroupId("DPKafkaProj");
		consumerConfigs.setWorkers(3);

		topicConfig.setConsumerGroupConfig(consumerConfigs);
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setTopic("my-topic");

		topicsConfig.setTopics(topicConfig);
		list.add(topicsConfig);
		sourceConfig.setTopics(list);
		sourceConfig.setBootStrapServers(Arrays.asList("localhost:9093"));

		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		doCallRealMethod().when(spySource).start(any());
		spySource.start(any());
		verify(spySource).start(any());
	}

	@Test
	void test_kafkaSource_start_execution_json_schemaType() throws Exception {

		List<TopicsConfig> list = new ArrayList<TopicsConfig>();
		TopicConfig topicConfig = new TopicConfig();
		KafkaSourceConfig sourceConfig = new KafkaSourceConfig();
		TopicsConfig topicsConfig = new TopicsConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		SchemaConfig schemaConfig = new SchemaConfig();
		schemaConfig.setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
		schemaConfig.setSchemaType("json");
		consumerConfigs.setAutoCommitInterval(Duration.ofMillis(1000));
		consumerConfigs.setAutoOffsetReset("earliest");
		consumerConfigs.setAutoCommit("false");
		consumerConfigs.setGroupId("DPKafkaProj");
		consumerConfigs.setWorkers(3);

		topicConfig.setConsumerGroupConfig(consumerConfigs);
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setTopic("my-topic");

		topicsConfig.setTopics(topicConfig);
		list.add(topicsConfig);
		sourceConfig.setTopics(list);
		sourceConfig.setBootStrapServers(Arrays.asList("localhost:9093"));
		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		ReflectionTestUtils.setField(spySource, "sourceConfig", sourceConfig);
		doCallRealMethod().when(spySource).start(any());
		spySource.start(any());
		verify(spySource).start(any());
	}

}
