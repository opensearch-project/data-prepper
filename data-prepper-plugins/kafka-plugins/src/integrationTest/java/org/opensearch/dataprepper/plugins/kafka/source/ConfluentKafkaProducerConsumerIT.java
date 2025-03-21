/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfluentKafkaProducerConsumerIT {
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

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    AuthConfig authConfig;

    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private KafkaSource kafkaSource;
    private TopicConsumerConfig topicConfig;
    private Counter counter;
    private List<Record> receivedRecords;

    private String bootstrapServers;
    private String topicName;
    private String username;
    private String password;
    private final int numRecordsProduced = 100;
    private AtomicInteger numRecordsReceived;
    private String currentTimeAsString;

    @BeforeEach
    public void setup() {
        currentTimeAsString = Instant.now().toString();
        numRecordsReceived = new AtomicInteger(0);
        sourceConfig = mock(KafkaSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        counter = mock(Counter.class);
        buffer = mock(Buffer.class);
        receivedRecords = new ArrayList<>();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(sourceConfig.getSchemaConfig()).thenReturn(null);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(pipelineDescription.getPipelineName()).thenReturn("testPipeline");
        try {
            doAnswer(args -> {
                Collection<Record<Event>> bufferedRecords = (Collection<Record<Event>>)args.getArgument(0);
                receivedRecords.addAll(bufferedRecords);
                Iterator iter = bufferedRecords.iterator();
                for (int i = 0; iter.hasNext(); i++) {
                        Record<Event> r = (Record<Event>)iter.next();
                        if (((Event)r.getData()).toJsonString().contains(currentTimeAsString+":")) {
                                numRecordsReceived.getAndAdd(1);
                        }
                }
                return null;
            }).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        } catch (Exception e){}
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        topicName = System.getProperty("tests.kafka.topic_name");
        username = System.getProperty("tests.kafka.username");
        password = System.getProperty("tests.kafka.password");
        topicConfig = mock(TopicConsumerConfig.class);
        when(topicConfig.getName()).thenReturn(topicName);
        when(topicConfig.getGroupId()).thenReturn("testGroupConf");
        when(topicConfig.getWorkers()).thenReturn(1);
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(20));
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(60));
        when(topicConfig.getRetryBackoff()).thenReturn(Duration.ofSeconds(1));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(100);
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(30));
        when(topicConfig.getFetchMaxBytes()).thenReturn(50L*1024*1024);
        when(topicConfig.getFetchMinBytes()).thenReturn(1L);
        when(topicConfig.getFetchMaxWait()).thenReturn(500);
        when(topicConfig.getMaxPartitionFetchBytes()).thenReturn(1024L*1024);
        when(topicConfig.getReconnectBackoff()).thenReturn(Duration.ofSeconds(10));
        when(sourceConfig.getTopics()).thenReturn((List) List.of(topicConfig));
        when(sourceConfig.getBootstrapServers()).thenReturn(List.of(bootstrapServers));
        encryptionConfig = mock(EncryptionConfig.class);
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(plainTextAuthConfig.getUsername()).thenReturn(username);
        when(plainTextAuthConfig.getPassword()).thenReturn(password);
    }

    @Test
    public void KafkaProduceConsumerTest() {
	produceRecords(bootstrapServers,  numRecordsProduced);
	consumeRecords(bootstrapServers);
	await().atMost(Duration.ofSeconds(20)).
	  untilAsserted(() -> assertThat(numRecordsReceived.get(), equalTo(numRecordsProduced)));
    }

    public void consumeRecords(String servers) {
        kafkaSource = new KafkaSource(sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription,
                null, pluginConfigObservable, awsCredentialsSupplier);
        kafkaSource.start(buffer);
    }

    public void produceRecords(String servers, int numRecords) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("request.timeout.ms", 20000);
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+username+"\" password=\""+password+"\";");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < numRecords; i++) {
                String key = "key"+String.valueOf(i);
                String value = currentTimeAsString+": TEST Value "+String.valueOf(i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
                producer.send(record);
            }
            producer.flush();

        }
    }

}
