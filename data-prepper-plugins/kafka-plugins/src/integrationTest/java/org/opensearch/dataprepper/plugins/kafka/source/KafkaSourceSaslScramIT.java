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
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.ScramAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaSourceSaslScramIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceSaslScramIT.class);
    private static final int TEST_ID = 123456;
    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private ScramAuthConfig scramAuthConfig;

    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    private AuthConfig authConfig;

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
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private TopicConsumerConfig jsonTopic;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private KafkaSource kafkaSource;

    private Counter counter;

    private List<Record> receivedRecords;

    private String bootstrapServers;
    private String testKey;
    private String testTopic;
    private String testGroup;

    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription,
                kafkaClusterConfigSupplier, pluginConfigObservable, awsCredentialsSupplier);
    }

    @BeforeEach
    public void setup() throws Throwable {
        sourceConfig = mock(KafkaSourceConfig.class);
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getScramAuthConfig()).thenReturn(scramAuthConfig);
        String username = System.getProperty("tests.kafka.authconfig.username");
        String password = System.getProperty("tests.kafka.authconfig.password");
        String mechanism = System.getProperty("tests.kafka.authconfig.mechanism");
        when(scramAuthConfig.getUsername()).thenReturn(username);
        when(scramAuthConfig.getPassword()).thenReturn(password);
        when(scramAuthConfig.getMechanism()).thenReturn(mechanism);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        buffer = mock(Buffer.class);
        encryptionConfig = mock(EncryptionConfig.class);
        receivedRecords = new ArrayList<>();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(executor);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(sourceConfig.getSchemaConfig()).thenReturn(null);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(pipelineDescription.getPipelineName()).thenReturn("testPipeline");
        try {
            doAnswer(args -> {
                Collection<Record<Event>> bufferedRecords = (Collection<Record<Event>>) args.getArgument(0);
                receivedRecords.addAll(bufferedRecords);
                return null;
            }).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        } catch (Exception e) {
        }

        testKey = RandomStringUtils.randomAlphabetic(5);
        testGroup = "TestGroup_" + RandomStringUtils.randomAlphabetic(6);
        testTopic = "TestJsonTopic_" + RandomStringUtils.randomAlphabetic(5);
        jsonTopic = mock(TopicConsumerConfig.class);
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
        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServers);
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        Throwable[] createThrowable = new Throwable[1];
        configureJasConfForSASLPlainText(props);
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(
                            Collections.singleton(new NewTopic(testTopic, 1, (short) 1)))
                    .all().whenComplete((v, throwable) -> {
                        created.set(true);
                        createThrowable[0] = throwable;
                    });
        }
        await().atMost(Duration.ofSeconds(30))
                .until(created::get);

        if(createThrowable[0] != null)
            throw createThrowable[0];
    }

    @AfterEach
    void tearDown() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configureJasConfForSASLPlainText(props);
        AtomicBoolean deleted = new AtomicBoolean(false);
        Throwable[] createThrowable = new Throwable[1];
        final String topicName = jsonTopic.getName();
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.deleteTopics(Collections.singleton(topicName))
                    .all().whenComplete((v, throwable) -> deleted.set(true));
        }
        await().atMost(Duration.ofSeconds(30))
                .until(deleted::get);
    }

    @Test
    public void TestJsonRecordsWithNullKey() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        when(sourceConfig.getTopics()).thenReturn((List) List.of(jsonTopic));
        kafkaSource = createObjectUnderTest();

        kafkaSource.start(buffer);
        testKey = null;
        produceJsonRecords(bootstrapServers, testTopic, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
        for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event) record.getData();
            EventMetadata metadata = event.getMetadata();
            Map<String, Object> map = event.toMap();
            assertThat(map.get("name"), equalTo("testName" + i));
            assertThat(map.get("id"), equalTo(TEST_ID + i));
            assertThat(map.get("status"), equalTo(true));
            assertThat(map.get("kafka_key"), equalTo(null));
            assertThat(metadata.getAttributes().get("kafka_topic"), equalTo(testTopic));
            assertThat(metadata.getAttributes().get("kafka_partition"), equalTo("0"));
        }
    }

    public void produceJsonRecords(final String servers, final String topicName, final int numRecords) {
        Properties props = new Properties();
        configureJasConfForSASLPlainText(props);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i < numRecords; i++) {
            String value = "{\"name\":\"testName" + i + "\", \"id\":" + (TEST_ID + i) + ", \"status\":true}";
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topicName, testKey, value);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        producer.close();
    }

    private void configureJasConfForSASLPlainText(final Properties props) {
        String username = System.getProperty("tests.kafka.authconfig.username");
        String password = System.getProperty("tests.kafka.authconfig.password");
        String mechanism = System.getProperty("tests.kafka.authconfig.mechanism");

        String jasConf = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jasConf);
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    }
}
