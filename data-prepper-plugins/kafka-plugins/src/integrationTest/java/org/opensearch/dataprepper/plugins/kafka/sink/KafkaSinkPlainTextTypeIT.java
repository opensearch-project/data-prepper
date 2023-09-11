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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaSinkPlainTextTypeIT {
    private static final int TEST_ID = 123456;
    @Mock
    private KafkaSinkConfig kafkaSinkConfig;

    @Mock
    private TopicConfig topicConfig;

    private KafkaSink kafkaSink;

    private String bootstrapServers;
    private String testTopic;

    private PluginSetting pluginSetting;

    @Mock
    private PluginFactory pluginFactory;

    private SinkContext sinkContext;

    @Mock
    private DlqProvider dlqProvider;

    @Mock
    private DlqWriter dlqWriter;

    @Mock
    private ExpressionEvaluator evaluator;

    private PlainTextAuthConfig plainTextAuthConfig;
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    private AuthConfig authConfig;

    private static final Properties props = new Properties();


    public KafkaSink createObjectUnderTest() {
        return new KafkaSink(pluginSetting, kafkaSinkConfig, pluginFactory, evaluator, sinkContext);
    }

    @BeforeEach
    public void setup() {
        plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        authConfig = mock(AuthConfig.class);

        evaluator = mock(ExpressionEvaluator.class);
        dlqWriter = mock(DlqWriter.class);
        dlqProvider = mock(DlqProvider.class);
        sinkContext = mock(SinkContext.class);
        pluginFactory = mock(PluginFactory.class);
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("name");
        when(pluginSetting.getPipelineName()).thenReturn("pipelinename");

        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.of(dlqWriter));

        kafkaSinkConfig = mock(KafkaSinkConfig.class);
        when(kafkaSinkConfig.getSchemaConfig()).thenReturn(null);
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT.toString());
        when(kafkaSinkConfig.getPartitionKey()).thenReturn("test-${name}");

        final String testGroup = "TestGroup_" + RandomStringUtils.randomAlphabetic(5);
        testTopic = "TestTopic_" + RandomStringUtils.randomAlphabetic(5);

        topicConfig = mock(TopicConfig.class);
        when(topicConfig.getName()).thenReturn(testTopic);
        when(topicConfig.getGroupId()).thenReturn(testGroup);
        when(topicConfig.getWorkers()).thenReturn(1);
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(45));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        when(kafkaSinkConfig.getBootStrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    @Test
    public void TestPollRecordsPlainText() throws Exception {

        configureJasConfForSASLPlainText();

        final int numRecords = 1;
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(topicConfig.isCreate()).thenReturn(false);
        when(kafkaSinkConfig.getTopic()).thenReturn(topicConfig);
        when(kafkaSinkConfig.getAuthConfig()).thenReturn(authConfig);
        kafkaSink = createObjectUnderTest();

        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = topicConfig.getName();

        createTopic(created, topicName);

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

        Thread.sleep(4000);

        consumeTestMessages(records);

        deleteTopic(created, topicName);
    }

    private void configureJasConfForSASLPlainText() {
        String username = System.getProperty("tests.kafka.authconfig.username");
        String password = System.getProperty("tests.kafka.authconfig.password");
        when(plainTextAuthConfig.getUsername()).thenReturn(username);
        when(plainTextAuthConfig.getPassword()).thenReturn(password);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);

        String jasConf = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jasConf);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
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
        while (created.get() != true) {
            Thread.sleep(1000);
        }
    }

    private void deleteTopic(AtomicBoolean created, String topicName) throws InterruptedException {
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

    private void consumeTestMessages(List<Record<Event>> recList) {

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getCommitInterval().toSecondsPart());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);

        kafkaConsumer.subscribe(Arrays.asList(topicConfig.getName()));

        pollRecords(recList, kafkaConsumer);
    }

    private void pollRecords(List<Record<Event>> recList, KafkaConsumer<String, String> kafkaConsumer) {
        int recListCounter = 0;
        boolean isPollNext = true;
        while (isPollNext) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            if (!records.isEmpty() && records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    Record<Event> recordEvent = recList.get(recListCounter);
                    String inputJsonStr = recordEvent.getData().toJsonString();

                    String recValue = record.value();
                    assertThat(recValue, CoreMatchers.containsString(inputJsonStr));
                    if (recListCounter + 1 == recList.size()) {
                        isPollNext = false;
                    }
                    recListCounter++;
                    break;
                }
            }
        }
    }
}