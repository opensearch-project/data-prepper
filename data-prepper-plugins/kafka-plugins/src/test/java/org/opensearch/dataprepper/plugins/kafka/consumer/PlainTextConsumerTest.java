package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doCallRealMethod;

class PlainTextConsumerTest {

    private PlainTextConsumer plainTextConsumer;

    @Mock
    private KafkaConsumer<String, String> kafkaPlainTextConsumer;

    @Mock
    private AtomicBoolean status;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private TopicConfig topicConfig;

    @Mock
    List<TopicConfig> mockList = new ArrayList<TopicConfig>();


    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SchemaConfig schemaConfig;

    @BeforeEach
    void setUp() throws Exception {
      //  plainTextConsumer = new PlainTextConsumer();
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if(data instanceof Map){
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            sourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            topicConfig = sourceConfig.getTopics().get(0);
        }
        pluginMetrics = mock(PluginMetrics.class);
        plainTextConsumer = new PlainTextConsumer(kafkaPlainTextConsumer, status, buffer, topicConfig,
                sourceConfig, "plaintext", pluginMetrics);
    }

    @Test
    void testPlainTextConsumeRecords_catch_block() {
        PlainTextConsumer spyConsumer = spy(plainTextConsumer);
        doCallRealMethod().when(spyConsumer).consumeRecords();
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();
    }

    private ConsumerRecords<String, String> buildConsumerRecords() throws Exception {
        String value = "test message";
       // JsonNode mapper = new ObjectMapper().readTree(value);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new LinkedHashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", "myvalue");
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", "myvalue");
        records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
        return new ConsumerRecords<>(records);
    }

   /* @Test
    void testPlainTextConsumerOnPartitionsAssigned() {
        List<TopicPartition> topicPartitions = buildTopicPartition();
        PlainTextConsumer spyConsumer = spy(plainTextConsumer);
        ReflectionTestUtils.setField(spyConsumer, "kafkaPlainTextConsumer", kafkaPlainTextConsumer);
        doCallRealMethod().when(spyConsumer).onPartitionsAssigned(topicPartitions);
        spyConsumer.onPartitionsAssigned(topicPartitions);
        verify(spyConsumer).onPartitionsAssigned(topicPartitions);
    }

    @Test
    void testPlainTextConsumerOnPartitionsRevoked() {
        PlainTextConsumer spyConsumer = spy(plainTextConsumer);
        ReflectionTestUtils.setField(spyConsumer, "kafkaPlainTextConsumer", kafkaPlainTextConsumer);
        doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
        spyConsumer.onPartitionsRevoked(anyList());
        verify(spyConsumer).onPartitionsRevoked(anyList());
    }*/

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