/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaSinkPlainTextTypeIT {
    private static final int TEST_ID = 123456;

    private KafkaSinkConfig kafkaSinkConfig;
    private TopicProducerConfig topicConfig;
    private KafkaSink kafkaSink;
    private String bootstrapServers;
    private String testTopic;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private PluginMetrics pluginMetrics;
    private SinkContext sinkContext;
    private DlqProvider dlqProvider;
    private DlqWriter dlqWriter;
    private ExpressionEvaluator evaluator;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private EncryptionConfig encryptionConfig;
    private Properties props;

    public KafkaSink createObjectUnderTest() {
        return new KafkaSink(pluginSetting, kafkaSinkConfig, pluginFactory, pluginMetrics, evaluator, sinkContext, awsCredentialsSupplier);
    }

    @BeforeEach
    public void setup() {
        props = new Properties();
        encryptionConfig = mock(EncryptionConfig.class);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);

        evaluator = mock(ExpressionEvaluator.class);
        dlqWriter = mock(DlqWriter.class);
        dlqProvider = mock(DlqProvider.class);
        sinkContext = mock(SinkContext.class);
        pluginFactory = mock(PluginFactory.class);
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(pluginSetting.getName()).thenReturn("name");
        when(pluginSetting.getPipelineName()).thenReturn("pipelinename");

        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.of(dlqWriter));

        kafkaSinkConfig = mock(KafkaSinkConfig.class);
        when(kafkaSinkConfig.getSchemaConfig()).thenReturn(null);
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT.toString());
        when(kafkaSinkConfig.getPartitionKey()).thenReturn("test-${name}");
        when(kafkaSinkConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        when(kafkaSinkConfig.getAuthConfig()).thenReturn(null);

        testTopic = "TestTopic_" + RandomStringUtils.randomAlphabetic(5);

        topicConfig = mock(TopicProducerConfig.class);
        when(topicConfig.getName()).thenReturn(testTopic);
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.isCreateTopic()).thenReturn(false);
        when(kafkaSinkConfig.getTopic()).thenReturn(topicConfig);

        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        when(kafkaSinkConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    @AfterEach
    public void tearDown() {
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.deleteTopics(Collections.singleton(testTopic))
                    .all().whenComplete((v, throwable) -> {});
        }
    }

    @Test
    public void TestPollRecordsPlainText() throws Exception {
        final int numRecords = 1;
        kafkaSink = createObjectUnderTest();

        AtomicBoolean created = new AtomicBoolean(false);
        createTopic(created, testTopic);

        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("name", "testName");
            eventData.put("id", "" + TEST_ID + i);
            final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
            records.add(new Record<>(event));
        }

        kafkaSink.doInitialize();
        kafkaSink.doOutput(records);

        consumeAndVerifyMessages(records);
    }

    private void createTopic(AtomicBoolean created, String topicName) throws InterruptedException {
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                adminClient.createTopics(
                        Collections.singleton(new NewTopic(topicName, 1, (short) 1)))
                        .all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.set(true);
        }
        await().atMost(Duration.ofSeconds(30)).until(created::get);
    }

    private void consumeAndVerifyMessages(List<Record<Event>> expectedRecords) {
        final String testGroup = "TestGroup_" + RandomStringUtils.randomAlphabetic(5);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, testGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            kafkaConsumer.subscribe(Collections.singletonList(testTopic));

            List<String> consumed = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    consumed.add(record.value());
                }
                assertThat(consumed.size(), equalTo(expectedRecords.size()));
            });

            for (int i = 0; i < expectedRecords.size(); i++) {
                String expectedJson = expectedRecords.get(i).getData().toJsonString();
                assertThat(consumed.get(i), equalTo(expectedJson));
            }
        }
    }
}
