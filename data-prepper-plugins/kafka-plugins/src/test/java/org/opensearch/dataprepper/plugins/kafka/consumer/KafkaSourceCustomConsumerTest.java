/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaSourceCustomConsumerTest {

    @Mock
    private KafkaConsumer<String, Object> kafkaConsumer;

    private AtomicBoolean status;

    private Buffer<Record<Event>> buffer;

    @Mock
    private KafkaSourceConfig sourceConfig;

    private ExecutorService callbackExecutor;
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private TopicConfig topicConfig;

    @Mock
    private KafkaTopicMetrics topicMetrics;

    private KafkaSourceCustomConsumer consumer;

    private ConsumerRecords consumerRecords;

    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private AtomicBoolean shutdownInProgress;
    private final String testKey1 = "testkey1";
    private final String testKey2 = "testkey2";
    private final String testValue1 = "testValue1";
    private final String testValue2 = "testValue2";
    private final Map<String, Object> testMap1 = Map.of("key1", "value1", "key2", 2);
    private final Map<String, Object> testMap2 = Map.of("key3", "value3", "key4", false);
    private final String testJsonValue1 = "{ \"key1\": \"value1\", \"key2\": 2}";
    private final String testJsonValue2 = "{ \"key3\": \"value3\", \"key4\": false}";
    private final int testPartition = 0;
    private final int testJsonPartition = 1;
    private Counter counter;

    @BeforeEach
    public void setUp() {
        kafkaConsumer = mock(KafkaConsumer.class);
        topicMetrics = mock(KafkaTopicMetrics.class);
        counter = mock(Counter.class);
        topicConfig = mock(TopicConfig.class);
        when(topicMetrics.getNumberOfPositiveAcknowledgements()).thenReturn(counter);
        when(topicMetrics.getNumberOfNegativeAcknowledgements()).thenReturn(counter);
        when(topicMetrics.getNumberOfNegativeAcknowledgements()).thenReturn(counter);
        when(topicMetrics.getNumberOfRecordsCommitted()).thenReturn(counter);
        when(topicMetrics.getNumberOfDeserializationErrors()).thenReturn(counter);
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(kafkaConsumer.committed(any(TopicPartition.class))).thenReturn(null);

        doAnswer((i)-> {return null;}).when(counter).increment();
        callbackExecutor = Executors.newFixedThreadPool(2); 
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(callbackExecutor, Duration.ofMillis(2000));

        sourceConfig = mock(KafkaSourceConfig.class);
        buffer = getBuffer();
        shutdownInProgress = new AtomicBoolean(false);
        when(topicConfig.getName()).thenReturn("topic1");
    }

    public KafkaSourceCustomConsumer createObjectUnderTest(String schemaType, boolean acknowledgementsEnabled) {
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(acknowledgementsEnabled);
        return new KafkaSourceCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topicConfig, schemaType, acknowledgementSetManager, topicMetrics);
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 10);
        integerHashMap.put("batch_size", 10);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }

    @Test
    public void testPlainTextConsumeRecords() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", false);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 2L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 2L);


        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
        }
    }

    @Test
    public void testPlainTextConsumeRecordsWithAcknowledgements() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", true);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 2L);
        });
        // This counter should not be incremented with acknowledgements
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 0L);
    }

    @Test
    public void testPlainTextConsumeRecordsWithNegativeAcknowledgements() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", true);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            event.getEventHandle().release(false);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);
    }

    @Test
    public void testJsonConsumeRecords() throws InterruptedException, Exception {
        String topic = topicConfig.getName();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        consumerRecords = createJsonRecords(topic);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("json", false);

        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));
        consumer.consumeRecords();
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 102L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 2L);

        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
        }
    }

    @Test
    public void testJsonDeserializationErrorWithAcknowledgements() throws Exception {
        String topic = topicConfig.getName();
        final ObjectMapper mapper = new ObjectMapper();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);

        consumer = createObjectUnderTest("json", true);
        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));

        // Send one json record
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        // Send non-json record that results in deser exception
        RecordDeserializationException exc = new RecordDeserializationException(new TopicPartition(topic, testJsonPartition),
                101L, "Deserializedation exception", new JsonParseException("Json parse exception"));
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(exc);
        consumer.consumeRecords();

        // Send one more json record
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 102L, testKey2,
                mapper.convertValue(testMap2, JsonNode.class));
        records.clear();
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record2));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(2, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(103L, offsetAndMetadata.offset());
        });
    }

    @Test
    public void testAwsGlueErrorWithAcknowledgements() throws Exception {
        String topic = topicConfig.getName();
        final ObjectMapper mapper = new ObjectMapper();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);

        consumer = createObjectUnderTest("json", true);
        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));

        // Send one json record
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        // Send non-json record that results in deser exception
        RecordDeserializationException exc = new RecordDeserializationException(new TopicPartition(topic, testJsonPartition),
                101L, "Deserializedation exception", new AWSSchemaRegistryException("AWS glue parse exception"));
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(exc);
        consumer.consumeRecords();

        // Send one more json record
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 102L, testKey2,
                mapper.convertValue(testMap2, JsonNode.class));
        records.clear();
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record2));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(2, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record: bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k,v)));
            }
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(103L, offsetAndMetadata.offset());
        });
    }

    private ConsumerRecords createPlainTextRecords(String topic, final long startOffset) {
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, startOffset, testKey1, testValue1);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic, testPartition, startOffset+1, testKey2, testValue2);
        records.put(new TopicPartition(topic, testPartition), Arrays.asList(record1, record2));
        return new ConsumerRecords(records);
    }

    private ConsumerRecords createJsonRecords(String topic) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 101L, testKey2, mapper.convertValue(testMap2, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1, record2));
        return new ConsumerRecords(records);
    }
}


