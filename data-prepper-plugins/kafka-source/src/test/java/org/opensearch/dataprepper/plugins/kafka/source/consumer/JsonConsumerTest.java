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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonConsumerTest {

	private JsonConsumer jsonConsumer;

	@Mock
	private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;

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
	private PluginMetrics pluginMetrics;

	@Mock
	private SchemaConfig schemaConfig;

	@BeforeEach
	void setUp() throws Exception {
		jsonConsumer = new JsonConsumer();		
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(topicsConfig.getTopic()).thenReturn(topicConfig);
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(topicConfig.getConsumerGroupConfig().getBufferDefaultTimeout()).thenReturn(Duration.ZERO);
		when(topicConfig.getName()).thenReturn(("my-topic"));
		when(topicConfig.getSchemaConfig().getRecordType()).thenReturn("string");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(topicConfig.getSchemaConfig().getSchemaType()).thenReturn("json");
	}

	@Test
	void testJsonConsumeRecords_catch_block() {
		JsonConsumer spyConsumer = spy(jsonConsumer);
		doCallRealMethod().when(spyConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, topicConfig,
				pluginMetrics);
		spyConsumer.consumeRecords(kafkaJsonConsumer, status, buffer, topicConfig, pluginMetrics);
		verify(spyConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, topicConfig, pluginMetrics);
	} 

	private ConsumerRecords<String, JsonNode> buildConsumerRecords() throws Exception {
		String value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		JsonNode mapper = new ObjectMapper().readTree(value);
		Map<TopicPartition, List<ConsumerRecord<String, JsonNode>>> records = new LinkedHashMap<>();
		ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
		ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
		records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
		return new ConsumerRecords<>(records);
	}

	@Test
	void testJsonConsumerOnPartitionsAssigned() {
		List<TopicPartition> topicPartitions = buildTopicPartition();
		JsonConsumer spyConsumer = spy(jsonConsumer);
		ReflectionTestUtils.setField(spyConsumer, "kafkaJsonConsumer", kafkaJsonConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsAssigned(topicPartitions);
		spyConsumer.onPartitionsAssigned(topicPartitions);
		verify(spyConsumer).onPartitionsAssigned(topicPartitions);
	}

	@Test
	void testJsonConsumerOnPartitionsRevoked() {
		JsonConsumer spyConsumer = spy(jsonConsumer);
		ReflectionTestUtils.setField(spyConsumer, "kafkaJsonConsumer", kafkaJsonConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
		spyConsumer.onPartitionsRevoked(anyList());
		verify(spyConsumer).onPartitionsRevoked(anyList());
	}

	@Test
	void testPublishRecordToBuffer_commitOffsets() throws Exception {
		when(schemaConfig.getSchemaType()).thenReturn("json");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		JsonConsumer spyConsumer = spy(jsonConsumer);
		doCallRealMethod().when(spyConsumer).commitOffsets(kafkaJsonConsumer);
		spyConsumer.commitOffsets(kafkaJsonConsumer);
		verify(spyConsumer).commitOffsets(kafkaJsonConsumer);
	}

	private List<TopicPartition> buildTopicPartition() {
		TopicPartition partition1 = new TopicPartition("my-topic", 1);
		TopicPartition partition2 = new TopicPartition("my-topic", 2);
		TopicPartition partition3 = new TopicPartition("my-topic", 3);
		return Arrays.asList(partition1, partition2, partition3);
	}
/*
	@Test
	void testJsonConsumer_write_to_buffer_positive_case() throws Exception{
	JsonConsumer spyConsumer = spy(jsonConsumer);
	String value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
	JsonNode mapper = new ObjectMapper().readTree(value);
	System.out.println("mapper::"+mapper);
	spyConsumer.writeToBuffer(mapper, buffer);
	verify(spyConsumer).writeToBuffer(mapper, buffer);
	}

	@Test
	void testJsonConsumer_write_to_buffer_exception_case() throws Exception{
	JsonConsumer spyConsumer = spy(jsonConsumer);
	assertThrows(Exception.class, () ->
	spyConsumer.writeToBuffer(new ObjectMapper().readTree("test"), buffer));
	}*/
}
