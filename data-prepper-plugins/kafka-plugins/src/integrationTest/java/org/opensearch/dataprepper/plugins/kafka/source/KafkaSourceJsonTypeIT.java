/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaSourceJsonTypeIT {
    private static final int TEST_ID = 123456;
    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private Buffer<Record<Event>> buffer;

    private List<TopicConfig> topicList;

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    private TopicConfig jsonTopic;

    private KafkaSource kafkaSource;

    private Counter counter;

    private List<Record> receivedRecords;

    private String bootstrapServers;
    private String testKey;
    private String testTopic;
    private String testGroup;

    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription);
    }

    @BeforeEach
    public void setup() {
        sourceConfig = mock(KafkaSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        buffer = mock(Buffer.class);
        encryptionConfig = mock(EncryptionConfig.class);
        receivedRecords = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(executor);
        pipelineDescription = mock(PipelineDescription.class);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(sourceConfig.getSchemaConfig()).thenReturn(null);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(pipelineDescription.getPipelineName()).thenReturn("testPipeline");
        try {
            doAnswer(args -> {
                Collection<Record<Event>> bufferedRecords = (Collection<Record<Event>>)args.getArgument(0);
                receivedRecords.addAll(bufferedRecords);
                return null;
            }).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        } catch (Exception e){}

        testKey = RandomStringUtils.randomAlphabetic(5);
        testGroup = "TestGroup_"+RandomStringUtils.randomAlphabetic(6);
        testTopic = "TestJsonTopic_"+RandomStringUtils.randomAlphabetic(5);
        jsonTopic = mock(TopicConfig.class);
        when(jsonTopic.getName()).thenReturn(testTopic);
        when(jsonTopic.getGroupId()).thenReturn(testGroup);
        when(jsonTopic.getWorkers()).thenReturn(1);
        when(jsonTopic.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(jsonTopic.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(jsonTopic.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(jsonTopic.getAutoCommit()).thenReturn(false);
        when(jsonTopic.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(jsonTopic.getAutoOffsetReset()).thenReturn("earliest");
        when(jsonTopic.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        when(sourceConfig.getBootStrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
    }

    @Test
    public void TestJsonRecordsWithNullKey() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        when(sourceConfig.getTopics()).thenReturn(List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                    Collections.singleton(new NewTopic(topicName, 1, (short)1)))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        while (created.get() != true) {
            Thread.sleep(1000);
        }
        kafkaSource.start(buffer);
        testKey = null;
        produceJsonRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(map.get("kafka_key"), equalTo(null));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.deleteTopics(Collections.singleton(topicName))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(false);
        }
        while (created.get() != false) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void TestJsonRecordsWithNegativeAcknowledgements() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.DISCARD);
        when(sourceConfig.getTopics()).thenReturn(List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(true);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                    Collections.singleton(new NewTopic(topicName, 1, (short)1)))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        while (created.get() != true) {
            Thread.sleep(1000);
        }
        kafkaSource.start(buffer);
        produceJsonRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
            event.getEventHandle().release(false);
        }
        receivedRecords.clear();
        Thread.sleep(10000);
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
            event.getEventHandle().release(true);
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.deleteTopics(Collections.singleton(topicName))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(false);
        }
        while (created.get() != false) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void TestJsonRecordsWithKafkaKeyModeDiscard() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.DISCARD);
        when(sourceConfig.getTopics()).thenReturn(List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                    Collections.singleton(new NewTopic(topicName, 1, (short)1)))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        while (created.get() != true) {
            Thread.sleep(1000);
        }
        kafkaSource.start(buffer);
        produceJsonRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.deleteTopics(Collections.singleton(topicName))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(false);
        }
        while (created.get() != false) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void TestJsonRecordsWithKafkaKeyModeAsField() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        when(sourceConfig.getTopics()).thenReturn(List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                    Collections.singleton(new NewTopic(topicName, 1, (short)1)))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        while (created.get() != true) {
            Thread.sleep(1000);
        }
        kafkaSource.start(buffer);
        produceJsonRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(map.get("kafka_key"), equalTo(testKey));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.deleteTopics(Collections.singleton(topicName))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(false);
        }
        while (created.get() != false) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void TestJsonRecordsWithKafkaKeyModeAsMetadata() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_METADATA);
        when(sourceConfig.getTopics()).thenReturn(List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                    Collections.singleton(new NewTopic(topicName, 1, (short)1)))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        while (created.get() != true) {
            Thread.sleep(1000);
        }
        kafkaSource.start(buffer);
        assertThat(kafkaSource.getConsumer().groupMetadata().groupId(), equalTo(testGroup));
        produceJsonRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName"+i));
            assertThat(map.get("id"), equalTo(TEST_ID+i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(metadata.getAttributes().get("kafka_key"), equalTo(testKey));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(topicName));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.deleteTopics(Collections.singleton(topicName))
                .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(false);
        }
        while (created.get() != false) {
            Thread.sleep(1000);
        }
    }

    public void produceJsonRecords(final String servers, final String topicName, final int numRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i < numRecords; i++) {
            String value = "{\"name\":\"testName"+i+"\", \"id\":"+(TEST_ID+i)+", \"status\":true}";
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topicName, testKey, value);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (Exception e){}
        }
        producer.close();
    }
}
