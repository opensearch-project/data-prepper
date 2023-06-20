package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;


public class KafkaSourceCustomConsumerTest {

    private KafkaConsumer<String, Object> kafkaConsumer;

    private AtomicBoolean status;

    private Buffer<Record<Object>> buffer;

    @Mock
    private KafkaSourceConfig sourceConfig;

    private TopicConfig topicConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    private String schemaType;

    private KafkaSourceCustomConsumer consumer;

    private final String TEST_PIPELINE_NAME = "test_pipeline";

    private Map<TopicPartition, List<ConsumerRecord<String, Object>>> records = new LinkedHashMap<TopicPartition, List<ConsumerRecord<String, Object>>>();


    @BeforeEach
    public void setUp() throws IOException {
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
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
        buffer = getBuffer();
        status = new AtomicBoolean(true);
        kafkaConsumer = mock(KafkaConsumer.class);
        schemaType = "plaintext";
        consumer = new KafkaSourceCustomConsumer(kafkaConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
    }

    private BlockingBuffer<Record<Object>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 10);
        integerHashMap.put("batch_size", 10);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }

    @Test
    public void testConsumeRecords() throws InterruptedException {

        String topic = topicConfig.getName();

        Thread producerThread = new Thread(() -> {
            setTopicData(topic);
        });
        producerThread.start();
        TimeUnit.SECONDS.sleep(1);
        ConsumerRecords consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any())).thenReturn(consumerRecords);
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();
        final Map.Entry<Collection<Record<Object>>, CheckpointState> bufferRecords = buffer.read(1000);
        Assertions.assertEquals(2, new ArrayList<>(bufferRecords.getKey()).size());
    }

    private void setTopicData(String topic) {
        ConsumerRecord<String, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, "mykey-1", "myvalue-1");
        ConsumerRecord<String, Object> record2 = new ConsumerRecord<>(topic, 0, 0L, "mykey-2", "myvalue-2");
        records.put(new TopicPartition(topic, 1), Arrays.asList(record1, record2));
    }

    @Test
    void testOnPartitionsRevoked() {
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        setTopicData(topicConfig.getName());
        final List<TopicPartition> topicPartitions = records.keySet().stream().collect(Collectors.toList());
        spyConsumer.onPartitionsRevoked(topicPartitions);
        verify(spyConsumer).onPartitionsRevoked(topicPartitions);
    }

    @Test
    void testOnPartitionsAssigned() {
        //Map<TopicPartition, List<ConsumerRecord<String, Object>>> records = new LinkedHashMap<>();
        ConsumerRecord<String, Object> record1 = new ConsumerRecord<>("my-topic-1", 0, 0L, "mykey-1", "myvalue-1");
        ConsumerRecord<String, Object> record2 = new ConsumerRecord<>("my-topic-1", 0, 0L, "mykey-2", "myvalue-2");
        //records.put(new TopicPartition("my-topic-1", 1), Arrays.asList(record1, record2));
        TopicPartition partition = new TopicPartition("my-topic-1", 1);
        //records.put(partition, Arrays.asList(record1, record2));

        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        doCallRealMethod().when(spyConsumer).onPartitionsAssigned(Arrays.asList(partition));

        spyConsumer.onPartitionsAssigned(Arrays.asList(partition));
        verify(spyConsumer).onPartitionsAssigned(Arrays.asList(partition));

        /*spyConsumer.onPartitionsRevoked(anyList());
        verify(spyConsumer).onPartitionsRevoked(anyList());*/
    }
}
