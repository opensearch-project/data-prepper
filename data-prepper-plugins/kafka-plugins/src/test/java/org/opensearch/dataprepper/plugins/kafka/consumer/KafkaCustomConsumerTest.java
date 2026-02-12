/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaCustomConsumerTest {
    private static final String TOPIC_NAME = "topic1";

    @Mock
    private KafkaConsumer<String, Object> kafkaConsumer;

    private AtomicBoolean status;

    private Buffer<Record<Event>> buffer;

    @Mock
    private Buffer<Record<Event>> mockBuffer;

    @Mock
    private KafkaConsumerConfig sourceConfig;

    private ScheduledExecutorService callbackExecutor;
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private TopicConsumerConfig topicConfig;

    @Mock
    private KafkaTopicConsumerMetrics topicMetrics;

    @Mock
    private PipelineDescription  pipelineDescription;

    @Mock
    private PauseConsumePredicate pauseConsumePredicate;

    private KafkaCustomConsumer consumer;

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
    @Mock
    private Counter posCounter;
    @Mock
    private Counter negCounter;
    @Mock
    private Counter overflowCounter;
    private Duration delayTime;
    private double posCount;
    private double negCount;
    private double overflowCount;
    private boolean paused;
    private boolean resumed;
    private String testStringHeader;

    @BeforeEach
    public void setUp() throws JsonProcessingException {
        delayTime = Duration.ofMillis(10);
        paused = false;
        resumed = false;
        kafkaConsumer = mock(KafkaConsumer.class);
        topicMetrics = mock(KafkaTopicConsumerMetrics.class);
        counter = mock(Counter.class);
        posCounter = mock(Counter.class);
        mockBuffer = mock(Buffer.class);
        negCounter = mock(Counter.class);
        overflowCounter = mock(Counter.class);
        topicConfig = mock(TopicConsumerConfig.class);
        when(topicMetrics.getNumberOfPositiveAcknowledgements()).thenReturn(posCounter);
        when(topicMetrics.getNumberOfNegativeAcknowledgements()).thenReturn(negCounter);
        when(topicMetrics.getNumberOfBufferSizeOverflows()).thenReturn(overflowCounter);
        when(topicMetrics.getNumberOfRecordsCommitted()).thenReturn(counter);
        when(topicMetrics.getNumberOfDeserializationErrors()).thenReturn(counter);
        when(topicMetrics.getNumberOfInvalidTimeStamps()).thenReturn(counter);
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(kafkaConsumer.committed(any(TopicPartition.class))).thenReturn(null);

        doAnswer((i)-> {
            paused = true;
            return null;
        }).when(kafkaConsumer).pause(any());

        doAnswer((i)-> {
            resumed = true;
            return null;
        }).when(kafkaConsumer).resume(any());

        doAnswer((i)-> {
            posCount += 1.0;
            return null;
        }).when(posCounter).increment();
        doAnswer((i)-> {
            negCount += 1.0;
            return null;
        }).when(negCounter).increment();
        doAnswer((i)-> {
            overflowCount += 1.0;
            return null;
        }).when(overflowCounter).increment();
        doAnswer((i)-> {return posCount;}).when(posCounter).count();
        doAnswer((i)-> {return negCount;}).when(negCounter).count();
        callbackExecutor = Executors.newScheduledThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(callbackExecutor, Duration.ofMillis(2000));

        sourceConfig = mock(KafkaConsumerConfig.class);
        buffer = getBuffer();
        shutdownInProgress = new AtomicBoolean(false);
        when(topicConfig.getName()).thenReturn(TOPIC_NAME);
    }

    public KafkaCustomConsumer createObjectUnderTestWithMockBuffer(String schemaType) {
        return new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, mockBuffer, sourceConfig, topicConfig, schemaType,
                acknowledgementSetManager, null, topicMetrics, pauseConsumePredicate);
    }

    public KafkaCustomConsumer createObjectUnderTest(String schemaType, boolean acknowledgementsEnabled) {
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(acknowledgementsEnabled);
        return new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topicConfig, schemaType,
                acknowledgementSetManager, null, topicMetrics, pauseConsumePredicate);
    }

    private BlockingBuffer<Record<Event>> getBuffer() throws JsonProcessingException {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 10);
        integerHashMap.put("batch_size", 10);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(integerHashMap);
        BlockingBufferConfig blockingBufferConfig = objectMapper.readValue(json, BlockingBufferConfig.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(blockingBufferConfig, pipelineDescription);
    }

    @Test
    public void testGetRecordTimeStamp() {
        ConsumerRecord<String, Object> consumerRecord1 = mock(ConsumerRecord.class);
        ConsumerRecord<String, Object> consumerRecord2 = mock(ConsumerRecord.class);
        ConsumerRecord<String, Object> consumerRecord3 = mock(ConsumerRecord.class);
        consumer = createObjectUnderTestWithMockBuffer("plaintext");
        long nowMs = Instant.now().toEpochMilli();
        long timestamp1 = nowMs - 5;
        when(consumerRecord1.timestamp()).thenReturn(timestamp1);
        when(consumerRecord1.partition()).thenReturn(1);
        assertThat(consumer.getRecordTimeStamp(consumerRecord1, nowMs), equalTo(timestamp1));
        long timestamp2 = nowMs + 5;
        when(consumerRecord2.timestamp()).thenReturn(timestamp2);
        when(consumerRecord2.partition()).thenReturn(1);
        assertThat(consumer.getRecordTimeStamp(consumerRecord2, nowMs), equalTo(timestamp1));
        long timestamp3 = nowMs + 10;
        when(consumerRecord3.timestamp()).thenReturn(timestamp3);
        when(consumerRecord3.partition()).thenReturn(2);
        assertThat(consumer.getRecordTimeStamp(consumerRecord3, nowMs), equalTo(nowMs));
    }

    @ParameterizedTest
    @MethodSource("provideExceptionsFromBufferWrite")
    public void testBufferOverflowPauseResume(final Exception bufferException) throws InterruptedException, Exception {
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofMillis(4000));
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        doAnswer((i)-> {
            if (!paused && !resumed)
                throw bufferException;
            buffer.writeAll(i.getArgument(0), i.getArgument(1));
            return null;
        }).when(mockBuffer).writeAll(any(), anyInt());

        doAnswer((i) -> {
            if (paused && !resumed)
                return List.of();
            return consumerRecords;
        }).when(kafkaConsumer).poll(any(Duration.class));
        consumer = createObjectUnderTestWithMockBuffer("plaintext");
        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        assertTrue(paused);
        assertTrue(resumed);

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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }

        verify(topicMetrics).recordTimeBetweenPolls();
    }

    @Test
    public void testPlainTextConsumeRecordsWithHeaders() throws InterruptedException {
        testStringHeader = UUID.randomUUID().toString();
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecordsWithHeaders(topic, 0L);
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
            Map<String, Object> attributes = event.getMetadata().getAttributes();
            Map<String, Object> kafkaHeaders = (Map<String, Object>) attributes.get("kafka_headers");
            assertThat(kafkaHeaders.get("test-string-header"), equalTo(testStringHeader));
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
                assertThat(kafkaHeaders.get("test-int-header"), notNullValue());
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
                assertThat(kafkaHeaders.get("test-double-header"), notNullValue());
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }

        verify(topicMetrics).recordTimeBetweenPolls();
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        await().atMost(delayTime.plusMillis(5000))
                .until(() -> {
                    return consumer.getTopicMetrics().getNumberOfPositiveAcknowledgements().count() == 1.0;
                });

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

        verify(topicMetrics).recordTimeBetweenPolls();
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(false);
        }
        // Wait for acknowledgement callback function to run
        await().atMost(delayTime.plusMillis(5000))
                .until(() -> {
                    return consumer.getTopicMetrics().getNumberOfNegativeAcknowledgements().count() == 1.0;
                });

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        verify(topicMetrics).recordTimeBetweenPolls();
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }

        verify(topicMetrics).recordTimeBetweenPolls();
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
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
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
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
    public void testCommitOffsets_RebalanceInProgressException_DoesNotClearOffsets() throws Exception {
        String topic = topicConfig.getName();
        TopicPartition topicPartition = new TopicPartition(topic, testPartition);

        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getCommitInterval()).thenReturn(Duration.ofMillis(0));

        consumer = createObjectUnderTest("plaintext", false);
        consumer.onPartitionsAssigned(List.of(topicPartition));

        consumerRecords = createPlainTextRecords(topic, 100L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        doThrow(new RebalanceInProgressException("Rebalance in progress"))
            .when(kafkaConsumer).commitSync(anyMap());

        consumer.consumeRecords();

        Map<TopicPartition, OffsetAndMetadata> offsetsBeforeCommit = new HashMap<>(consumer.getOffsetsToCommit());
        Assertions.assertFalse(offsetsBeforeCommit.isEmpty(), "Offsets should be populated after consuming records");
        Assertions.assertEquals(102L, offsetsBeforeCommit.get(topicPartition).offset());

        Thread testThread = new Thread(() -> {
            try {
                java.lang.reflect.Method method = consumer.getClass().getDeclaredMethod("commitOffsets", boolean.class);
                method.setAccessible(true);
                method.invoke(consumer, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        testThread.start();
        testThread.join(5000);

        Map<TopicPartition, OffsetAndMetadata> offsetsAfterFailedCommit = consumer.getOffsetsToCommit();
        Assertions.assertFalse(offsetsAfterFailedCommit.isEmpty(),
            "Offsets should NOT be cleared after RebalanceInProgressException");
        Assertions.assertEquals(offsetsBeforeCommit.get(topicPartition).offset(),
            offsetsAfterFailedCommit.get(topicPartition).offset(),
            "Offset value should remain unchanged for retry after rebalance completes");
    }

    @Test
    public void testCommitOffsets_OtherException_ClearsOffsets() throws Exception {
        String topic = topicConfig.getName();
        TopicPartition topicPartition = new TopicPartition(topic, testPartition);

        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getCommitInterval()).thenReturn(Duration.ofMillis(0));

        consumer = createObjectUnderTest("plaintext", false);
        consumer.onPartitionsAssigned(List.of(topicPartition));

        consumerRecords = createPlainTextRecords(topic, 100L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        doThrow(new RuntimeException("Generic commit failure"))
            .when(kafkaConsumer).commitSync(anyMap());

        consumer.consumeRecords();

        Assertions.assertFalse(consumer.getOffsetsToCommit().isEmpty(),
            "Offsets should be populated after consuming records");

        Thread testThread = new Thread(() -> {
            try {
                java.lang.reflect.Method method = consumer.getClass().getDeclaredMethod("commitOffsets", boolean.class);
                method.setAccessible(true);
                method.invoke(consumer, true);
            } catch (Exception e) {
            }
        });
        testThread.start();
        testThread.join(5000);

        Map<TopicPartition, OffsetAndMetadata> offsetsAfterFailedCommit = consumer.getOffsetsToCommit();
        Assertions.assertTrue(offsetsAfterFailedCommit.isEmpty(),
            "Offsets should be cleared after non-rebalance exception");
    }

    private ConsumerRecords createPlainTextRecords(String topic, final long startOffset) {
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, startOffset, testKey1, testValue1);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic, testPartition, startOffset+1, testKey2, testValue2);
        records.put(new TopicPartition(topic, testPartition), Arrays.asList(record1, record2));
        return new ConsumerRecords(records);
    }

    private ConsumerRecords createPlainTextRecordsWithHeaders(String topic, final long startOffset) {
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        RecordHeaders headers1 = new RecordHeaders();
        RecordHeaders headers2 = new RecordHeaders();

        // Adding a String header
        headers1.add("test-string-header", testStringHeader.getBytes(StandardCharsets.UTF_8));
        headers2.add("test-string-header", testStringHeader.getBytes(StandardCharsets.UTF_8));

        // Adding an Integer header (stored as 4 bytes)
        int testIntHeader = 5;
        byte[] intBytes = ByteBuffer.allocate(4).putInt(testIntHeader).array();
        headers1.add("test-int-header", intBytes);
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, startOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, 0L, 0, 0,testKey1, testValue1, headers1);
        double testDoubleHeader = 5.125d;
        byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(testDoubleHeader).array();
        headers2.add("test-double-header", doubleBytes);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic, testPartition, startOffset+1, System.currentTimeMillis(), TimestampType.CREATE_TIME, 0L, 0, 0, testKey2, testValue2, headers2);
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

    private static Stream<Arguments> provideExceptionsFromBufferWrite() {
        return Stream.of(
                Arguments.of(new SizeOverflowException("size overflow")),
                Arguments.of(new TimeoutException()));
    }
}


