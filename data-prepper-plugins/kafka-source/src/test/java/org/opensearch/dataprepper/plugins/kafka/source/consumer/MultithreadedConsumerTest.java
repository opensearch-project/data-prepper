package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerConfigs;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicsConfig;
import org.springframework.test.util.ReflectionTestUtils;


@SuppressWarnings("deprecation")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MultithreadedConsumerTest {

	@Mock
	private MultithreadedConsumer multithreadconsumer;

	private Properties properties;

	@Mock
	private KafkaSourceConfig sourceConfig;
	
	@Mock
	private TopicsConfig topicsConfig;
	
	@Mock
	List<TopicsConfig> mockList = new ArrayList<TopicsConfig>();
	
	@Mock
	private TopicConfig topicConfig;
	
	@Mock
	private SchemaConfig schemaConfig;

	@Mock
	private PluginMetrics pluginMetrics;

	@Mock
	private Buffer<Record<Object>> buffer;

	@Mock
	private Collection<TopicPartition> partitions;

	@Mock
	private KafkaConsumer<String, String> kafkaConsumer;

	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(topicsConfig.getTopic()).thenReturn(topicConfig);
		when(topicConfig.getSchemaConfig()).thenReturn(schemaConfig);
		
		properties = new Properties();
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(topicConfig.getName()).thenReturn("my-topic");
		when(schemaConfig.getRecordType()).thenReturn("string");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
	}

	@Test
	void testMultithreadedConsumer_run_execution_plaintext_schemaType() {
		
		ConsumerRecords<String, String> consumerRecords = buildConsumerRecords();
		when(schemaConfig.getSchemaType()).thenReturn("plaintext");
		when(kafkaConsumer.poll(Duration.ofMillis(100))).thenReturn(consumerRecords);

		MultithreadedConsumer spyConsumer = spy(multithreadconsumer);
		ReflectionTestUtils.setField(spyConsumer, "plainTextConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).call();
		spyConsumer.call();
		verify(spyConsumer).call();
	}

	@Test
	void testMultithreadedConsumer_run_execution_json_schemaType() {
		multithreadconsumer = new MultithreadedConsumer("DPKafkaProj", "DPKafkaProj", properties, topicConfig, buffer,
				pluginMetrics);
		ConsumerRecords<String, String> consumerRecords = buildConsumerRecords();
		when(schemaConfig.getSchemaType()).thenReturn("json");
		when(kafkaConsumer.poll(Duration.ofMillis(100))).thenReturn(consumerRecords);

		MultithreadedConsumer spyConsumer = spy(multithreadconsumer);
		ReflectionTestUtils.setField(spyConsumer, "jsonConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).call();
		spyConsumer.call();
		verify(spyConsumer).call();
	}

	@Test
	void testMultithreadedConsumer_publishRecordToBuffer_catch_block() {
		ConsumerRecords<String, String> consumerRecords = buildConsumerRecords();
		when(kafkaConsumer.poll(Duration.ofMillis(100))).thenReturn(consumerRecords);

		MultithreadedConsumer spyConsumer = spy(multithreadconsumer);
		ReflectionTestUtils.setField(spyConsumer, "plainTextConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).call();
		spyConsumer.call();
		verify(spyConsumer).call();
	}

	@Test
	void testMultithreadedConsumer_run_execution_catch_block() {
		when(kafkaConsumer.poll(Duration.ofMillis(100))).thenReturn(null);
		MultithreadedConsumer spyConsumer = spy(multithreadconsumer);
		ReflectionTestUtils.setField(spyConsumer, "plainTextConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).call();
		spyConsumer.call();
		verify(spyConsumer).call();
	}

	private ConsumerRecords<String, String> buildConsumerRecords() {
		String value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new LinkedHashMap<>();
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", value);
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", value);
		records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
		return new ConsumerRecords<>(records);
	}

	

	@Test
	void testShutdownConsumer_execution() {
		multithreadconsumer = new MultithreadedConsumer("DPKafkaProj", "DPKafkaProj", properties, topicConfig, buffer,
				pluginMetrics);
		MultithreadedConsumer spyConsumer = spy(multithreadconsumer);
		doCallRealMethod().when(spyConsumer).shutdownConsumer();
		spyConsumer.shutdownConsumer();
		verify(spyConsumer).shutdownConsumer();
	}
}
