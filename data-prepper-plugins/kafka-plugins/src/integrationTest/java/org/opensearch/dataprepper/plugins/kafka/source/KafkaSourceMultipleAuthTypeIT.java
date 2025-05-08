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
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaSourceMultipleAuthTypeIT {
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
    private TopicConsumerConfig plainTextTopic;

    @Mock
    private AuthConfig authConfig;

    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private TopicConfig jsonTopic;
    private TopicConfig avroTopic;

    private KafkaSource kafkaSource;

    private Counter counter;

    private List<Record> receivedRecords;

    private String bootstrapServers;
    private String saslsslBootstrapServers;
    private String saslplainBootstrapServers;
    private String sslBootstrapServers;
    private String kafkaUsername;
    private String kafkaPassword;

    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(
                sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription,
                kafkaClusterConfigSupplier, pluginConfigObservable, awsCredentialsSupplier);
    }

    @BeforeEach
    public void setup() {
        sourceConfig = mock(KafkaSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        buffer = mock(Buffer.class);
        encryptionConfig = mock(EncryptionConfig.class);
        receivedRecords = new ArrayList<>();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
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

        final String testGroup = "TestGroup_"+RandomStringUtils.randomAlphabetic(6);
        final String testTopic = "TestTopic_"+RandomStringUtils.randomAlphabetic(5);
        plainTextTopic = mock(TopicConsumerConfig.class);
        when(plainTextTopic.getName()).thenReturn(testTopic);
        when(plainTextTopic.getGroupId()).thenReturn(testGroup);
        when(plainTextTopic.getWorkers()).thenReturn(1);
        when(plainTextTopic.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(plainTextTopic.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(plainTextTopic.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(plainTextTopic.getAutoCommit()).thenReturn(false);
        when(plainTextTopic.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(plainTextTopic.getAutoOffsetReset()).thenReturn("earliest");
        when(plainTextTopic.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        saslsslBootstrapServers = System.getProperty("tests.kafka.saslssl_bootstrap_servers");
        saslplainBootstrapServers = System.getProperty("tests.kafka.saslplain_bootstrap_servers");
        sslBootstrapServers = System.getProperty("tests.kafka.ssl_bootstrap_servers");
        kafkaUsername = System.getProperty("tests.kafka.username");
        kafkaPassword = System.getProperty("tests.kafka.password");
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
    }

    @Test
    public void TestPlainTextWithNoAuthKafkaNoEncryptionWithNoAuthSchemaRegistry() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(plainTextTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getTopics()).thenReturn((List) List.of(plainTextTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        kafkaSource = createObjectUnderTest();
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = plainTextTopic.getName();
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
        produceKafkaRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
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
    public void TestPlainTextWithAuthKafkaNoEncryptionWithNoAuthSchemaRegistry() throws Exception {
        final int numRecords = 1;
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(plainTextTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getTopics()).thenReturn((List) List.of(plainTextTopic));
        plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(plainTextAuthConfig.getUsername()).thenReturn(kafkaUsername);
        when(plainTextAuthConfig.getPassword()).thenReturn(kafkaPassword);
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(saslplainBootstrapServers));
        kafkaSource = createObjectUnderTest();
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = plainTextTopic.getName();
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
        produceKafkaRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
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
    public void TestPlainTextWithNoAuthKafkaEncryptionWithNoAuthSchemaRegistry() throws Exception {
        final int numRecords = 1;
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(null);
        when(encryptionConfig.getInsecure()).thenReturn(true);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(plainTextTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(sslBootstrapServers));
        when(sourceConfig.getTopics()).thenReturn((List) List.of(plainTextTopic));
        kafkaSource = createObjectUnderTest();
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = plainTextTopic.getName();
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
        produceKafkaRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
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
    public void TestPlainTextWithAuthKafkaEncryptionWithNoAuthSchemaRegistry() throws Exception {
        final int numRecords = 1;
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(plainTextAuthConfig.getUsername()).thenReturn(kafkaUsername);
        when(plainTextAuthConfig.getPassword()).thenReturn(kafkaPassword);
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(encryptionConfig.getInsecure()).thenReturn(true);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(plainTextTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(saslsslBootstrapServers));
        when(sourceConfig.getTopics()).thenReturn((List) List.of(plainTextTopic));
        kafkaSource = createObjectUnderTest();
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = plainTextTopic.getName();
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
        produceKafkaRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
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

    public void produceKafkaRecords(final String servers, final String topicName, final int numRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i < numRecords; i++) {
            String key = RandomStringUtils.randomAlphabetic(5);
            String value = RandomStringUtils.randomAlphabetic(10);
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(topicName, key, value);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (Exception e){}
        }
        producer.close();
    }
}
