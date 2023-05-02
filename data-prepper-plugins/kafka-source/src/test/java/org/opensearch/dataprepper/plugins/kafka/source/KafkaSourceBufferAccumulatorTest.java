package org.opensearch.dataprepper.plugins.kafka.source;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerConfigs;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicsConfig;


@SuppressWarnings("deprecation")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceBufferAccumulatorTest {

	@Mock
	private KafkaSourceBufferAccumulator<String, String> buffer;

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
	private Buffer<Record<Object>> record;

	@Mock
	List<Record<Object>> kafkaRecords;

	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(topicsConfig.getTopic()).thenReturn(topicConfig);
		when(topicConfig.getSchemaConfig()).thenReturn(schemaConfig);
		
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		
		when(schemaConfig.getRecordType()).thenReturn("string");
		buffer = new KafkaSourceBufferAccumulator<>(topicConfig, pluginMetrics);
	}

	@Test
	void testWriteEventOrStringToBuffer_plaintext_schemaType() throws Exception {
		TopicConfig topicConfig = new TopicConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		consumerConfigs.setBufferDefaultTimeout(Duration.ofMillis(100));
		schemaConfig.setSchemaType("plaintext");
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setConsumerGroupConfig(consumerConfigs);

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord("anyString", topicConfig);
		spyBuffer.getEventRecord("anyString", topicConfig);
		verify(spyBuffer).getEventRecord("anyString", topicConfig);
	}

	@Test
	void testWriteEventOrStringToBuffer_json_schemaType() throws Exception {
		String json = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		TopicConfig topicConfig = new TopicConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		consumerConfigs.setBufferDefaultTimeout(Duration.ofMillis(100));
		schemaConfig.setSchemaType("json");
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setConsumerGroupConfig(consumerConfigs);

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord(json, topicConfig);
		spyBuffer.getEventRecord(json, topicConfig);
		verify(spyBuffer).getEventRecord(json, topicConfig);
	}

	@Test
	void testWriteEventOrStringToBuffer_json_schemaType_catch_block() throws Exception {
		TopicConfig topicConfig = new TopicConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		consumerConfigs.setBufferDefaultTimeout(Duration.ofMillis(100));
		schemaConfig.setSchemaType("json");
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setConsumerGroupConfig(consumerConfigs);

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord("anyString", topicConfig);
		spyBuffer.getEventRecord("anyString", topicConfig);
		verify(spyBuffer).getEventRecord("anyString", topicConfig);
	}

	@Test
	void testWriteEventOrStringToBuffer_plaintext_schemaType_catch_block() throws Exception {
		TopicConfig topicConfig = new TopicConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		consumerConfigs.setBufferDefaultTimeout(Duration.ofMillis(100));
		schemaConfig.setSchemaType("plaintext");
		topicConfig.setSchemaConfig(schemaConfig);
		topicConfig.setConsumerGroupConfig(consumerConfigs);

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord(null, topicConfig);
		spyBuffer.getEventRecord(null, topicConfig);
		verify(spyBuffer).getEventRecord(null, topicConfig);
	}

	@Test
	void testwriteAllRecordToBuffer()throws Exception{
		TopicConfig topicConfig = new TopicConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		ConsumerConfigs consumerConfigs = new ConsumerConfigs();
		consumerConfigs.setBufferDefaultTimeout(Duration.ofMillis(100));
		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeAllRecordToBuffer(kafkaRecords, record, topicConfig);
		spyBuffer.writeAllRecordToBuffer(kafkaRecords, record,topicConfig);
		verify(spyBuffer).writeAllRecordToBuffer(kafkaRecords, record,topicConfig);
	}

}
