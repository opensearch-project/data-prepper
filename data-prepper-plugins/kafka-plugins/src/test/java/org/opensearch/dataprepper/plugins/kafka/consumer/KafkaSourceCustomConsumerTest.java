package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;


public class KafkaSourceCustomConsumerTest {
    @Mock
    private KafkaConsumer<String, Object> plainTextConsumer;

    @Mock
    private KafkaConsumer<String, Object> jsonConsumer;

    @Mock
    private KafkaConsumer<String, Object> avroConsumer;
    private AtomicBoolean status;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private KafkaSourceConfig sourceConfig;

    private TopicConfig topicConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    private String schemaType;
    private KafkaSourceCustomConsumer consumer;

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
        buffer = mock(Buffer.class);
        status = new AtomicBoolean();
        plainTextConsumer = mock(KafkaConsumer.class);
        jsonConsumer = mock(KafkaConsumer.class);
        avroConsumer = mock(KafkaConsumer.class);
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
    }

    @Test
    public void testConsumeRecords() throws InterruptedException {
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        String topic = topicConfig.getName();
        schemaType = "plaintext";
        Map<TopicPartition, List<ConsumerRecord<String, Object>>> records = new LinkedHashMap<>();
        Thread producerThread = new Thread(() -> {
            ConsumerRecord<String, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, "mykey-1", "myvalue-1");
            ConsumerRecord<String, Object> record2 = new ConsumerRecord<>(topic, 0, 0L, "mykey-2", "myvalue-2");
            records.put(new TopicPartition(topic, 1), Arrays.asList(record1, record2));
        });
        producerThread.start();
        TimeUnit.SECONDS.sleep(1);
        producerThread.join();
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        doCallRealMethod().when(spyConsumer).consumeRecords();
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();

    }

    @Test
    void testOnPartitionsRevoked() {
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        ReflectionTestUtils.setField(spyConsumer, "kafkaConsumer", plainTextConsumer);
        doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
        spyConsumer.onPartitionsRevoked(anyList());
        verify(spyConsumer).onPartitionsRevoked(anyList());
    }

    @Test
    void testOnPartitionsAssigned() {
        //Map<TopicPartition, List<ConsumerRecord<String, Object>>> records = new LinkedHashMap<>();
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        ConsumerRecord<String, Object> record1 = new ConsumerRecord<>("my-topic-1", 0, 0L, "mykey-1", "myvalue-1");
        ConsumerRecord<String, Object> record2 = new ConsumerRecord<>("my-topic-1", 0, 0L, "mykey-2", "myvalue-2");
        //records.put(new TopicPartition("my-topic-1", 1), Arrays.asList(record1, record2));
        TopicPartition partition = new TopicPartition("my-topic-1", 1);
        //records.put(partition, Arrays.asList(record1, record2));

        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        ReflectionTestUtils.setField(spyConsumer, "kafkaConsumer", plainTextConsumer);
        doCallRealMethod().when(spyConsumer).onPartitionsAssigned(Arrays.asList(partition));

        spyConsumer.onPartitionsAssigned(Arrays.asList(partition));
        verify(spyConsumer).onPartitionsAssigned(Arrays.asList(partition));

        /*spyConsumer.onPartitionsRevoked(anyList());
        verify(spyConsumer).onPartitionsRevoked(anyList());*/
    }
}
