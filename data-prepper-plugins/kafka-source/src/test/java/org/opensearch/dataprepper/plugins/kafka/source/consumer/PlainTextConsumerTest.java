package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
@MockitoSettings(strictness = Strictness.LENIENT)
class PlainTextConsumerTest {

	private PlainTextConsumer textConsumer;
	
	@Mock
	private KafkaConsumer<String, String> kafkaConsumer;
	
	@Mock
	private AtomicBoolean status;
	
	@Mock
	private Buffer<Record<Object>> buffer;
	
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
	
	@BeforeEach
	void setUp() throws Exception {
		textConsumer = new PlainTextConsumer();
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(topicsConfig.getTopic()).thenReturn(topicConfig);
		
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(topicConfig.getConsumerGroupConfig().getBufferDefaultTimeout()).thenReturn(Duration.ZERO);
		when(topicConfig.getName()).thenReturn("my-topic");
		when(topicConfig.getSchemaConfig().getRecordType()).thenReturn("string");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(topicConfig.getSchemaConfig().getSchemaType()).thenReturn("plaintext");
	}
	@Test
	void testPlainTextConsumerRecords_catch_block() {
		PlainTextConsumer spyConsumer = spy(textConsumer);
		doCallRealMethod().when(spyConsumer).consumeRecords(kafkaConsumer, status, buffer, topicConfig, pluginMetrics);
		spyConsumer.consumeRecords(kafkaConsumer, status, buffer, topicConfig, pluginMetrics);
		verify(spyConsumer).consumeRecords(kafkaConsumer, status, buffer, topicConfig, pluginMetrics);
	}

	private ConsumerRecords<String, String> buildConsumerRecords() {
		String value = "This is plain text consumer records";
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new LinkedHashMap<>();
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", value);
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", value);
		records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
		return new ConsumerRecords<>(records);
	}
	@Test
	void testPlainTextConsumerOnPartitionsAssigned() {
		List<TopicPartition> topicPartitions = buildTopicPartition();
		PlainTextConsumer spyConsumer = spy(textConsumer);
		ReflectionTestUtils.setField(spyConsumer, "plainTxtConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsAssigned(topicPartitions);
		spyConsumer.onPartitionsAssigned(topicPartitions);
		verify(spyConsumer).onPartitionsAssigned(topicPartitions);
	}
	
	@Test
	void testPlainTextConsumerOnPartitionsRevoked() {
		PlainTextConsumer spyConsumer = spy(textConsumer);
		ReflectionTestUtils.setField(spyConsumer, "plainTxtConsumer", kafkaConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
		spyConsumer.onPartitionsRevoked(anyList());
		verify(spyConsumer).onPartitionsRevoked(anyList());
	}

	@Test
	void testPublishRecordToBuffer_commitOffsets() throws Exception {
		when(schemaConfig.getSchemaType()).thenReturn("plaintext");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		PlainTextConsumer spyConsumer = spy(textConsumer);
		doCallRealMethod().when(spyConsumer).commitOffsets(kafkaConsumer);
		spyConsumer.commitOffsets(kafkaConsumer);
		verify(spyConsumer).commitOffsets(kafkaConsumer);
	}

	private List<TopicPartition> buildTopicPartition(){
		TopicPartition partition1 = new TopicPartition("my-topic", 1);
		TopicPartition partition2 = new TopicPartition("my-topic", 2);
		TopicPartition partition3 = new TopicPartition("my-topic", 3);
		return Arrays.asList(partition1, partition2, partition3);
	}
}
