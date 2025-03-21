/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.core.instrument.Counter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
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
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfluentKafkaProducerConsumerWithSchemaRegistryIT {
    public static class AvroRecord {
          @JsonProperty
          public String message;

          @JsonProperty
          public Integer ident;

          @JsonProperty
          public Number score;

          public AvroRecord() {}

          public AvroRecord(String message, Integer ident, Number score) {
             this.message = message;
             this.ident = ident;
             this.score = score;
          }
    };
    public static class UserRecord {
          @JsonProperty
          public String name;

          @JsonProperty
          public Integer id;

          @JsonProperty
          public Number value;

          public UserRecord() {}

          public UserRecord(String name, Integer id, Number value) {
             this.name = name;
             this.id = id;
             this.value = value;
          }
    };

    private int testId;
    private double testValue;

    @Mock
    private KafkaSourceConfig jsonSourceConfig;

    @Mock
    private KafkaSourceConfig avroSourceConfig;

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
    SchemaConfig schemaConfig;

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
    private TopicConsumerConfig jsonTopicConfig;
    private TopicConsumerConfig avroTopicConfig;
    private Counter counter;
    private List<Record> receivedRecords;

    private String bootstrapServers;
    private String schemaRegistryUrl;
    private String schemaRegistryUserInfo;
    private String jsonTopicName;
    private String avroTopicName;
    private String username;
    private String password;
    private final int numRecordsProduced = 100;
    private AtomicInteger numRecordsReceived;
    private String testMessage;

    @BeforeEach
    public void setup() {
        numRecordsReceived = new AtomicInteger(0);
        testMessage = "M_"+RandomStringUtils.randomAlphabetic(5)+"_M_";
        jsonSourceConfig = mock(KafkaSourceConfig.class);
        avroSourceConfig = mock(KafkaSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        pluginConfigObservable = mock(PluginConfigObservable.class);
	Random random = new Random();
	testId = random.nextInt();
	testValue = random.nextDouble();
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        counter = mock(Counter.class);
        buffer = mock(Buffer.class);
        schemaConfig = mock(SchemaConfig.class);
        receivedRecords = new ArrayList<>();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(schemaConfig.getType()).thenReturn(SchemaRegistryType.CONFLUENT);
        when(schemaConfig.getRegistryURL()).thenReturn("https://pkc-lzvrd.us-west4.gcp.confluent.cloud:443");

        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(pipelineDescription.getPipelineName()).thenReturn("testPipeline");
        try {
            doAnswer(args -> {
                Collection<Record<Event>> bufferedRecords = (Collection<Record<Event>>)args.getArgument(0);
                receivedRecords.addAll(bufferedRecords);
                Iterator iter = bufferedRecords.iterator();
                for (int i = 0; iter.hasNext(); i++) {
                        Record<Event> record = (Record<Event>)iter.next();
                        Event event = (Event) record.getData();
                        String name = event.get("name", String.class);
                        Number value = event.get("value", Number.class);
                        if (event.toJsonString().contains(testMessage)) {
                                numRecordsReceived.getAndAdd(1);
                        }
                }
                return null;
            }).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        } catch (Exception e){}
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        schemaRegistryUrl = System.getProperty("tests.kafka.schema_registry_url");
        schemaRegistryUserInfo = System.getProperty("tests.kafka.schema_registry_userinfo");
        username = System.getProperty("tests.kafka.username");
        password = System.getProperty("tests.kafka.password");

        jsonTopicConfig = mock(TopicConsumerConfig.class);
        jsonTopicName = System.getProperty("tests.kafka.json_topic_name");
        when(jsonTopicConfig.getName()).thenReturn(jsonTopicName);
        when(jsonTopicConfig.getGroupId()).thenReturn("testGroupConf");
        when(jsonTopicConfig.getWorkers()).thenReturn(1);
        when(jsonTopicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(jsonTopicConfig.getAutoCommit()).thenReturn(false);
        when(jsonTopicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(jsonTopicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(jsonTopicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(30));
        when(jsonTopicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(jsonTopicConfig.getConsumerMaxPollRecords()).thenReturn(100);
        when(jsonTopicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(15));

        avroTopicConfig = mock(TopicConsumerConfig.class);
        avroTopicName = System.getProperty("tests.kafka.avro_topic_name");
        when(avroTopicConfig.getName()).thenReturn(avroTopicName);
        when(avroTopicConfig.getGroupId()).thenReturn("testGroupConf");
        when(avroTopicConfig.getWorkers()).thenReturn(1);
        when(avroTopicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(avroTopicConfig.getAutoCommit()).thenReturn(false);
        when(avroTopicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(avroTopicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(avroTopicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(30));
        when(avroTopicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(avroTopicConfig.getConsumerMaxPollRecords()).thenReturn(100);
        when(avroTopicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(15));

        encryptionConfig = mock(EncryptionConfig.class);

        when(jsonSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(jsonSourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(jsonSourceConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(jsonSourceConfig.getTopics()).thenReturn((List) List.of(jsonTopicConfig));
        when(jsonSourceConfig.getBootstrapServers()).thenReturn(List.of(bootstrapServers));
        when(jsonSourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);

        when(avroSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(avroSourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(avroSourceConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(avroSourceConfig.getTopics()).thenReturn((List) List.of(avroTopicConfig));
        when(avroSourceConfig.getBootstrapServers()).thenReturn(List.of(bootstrapServers));
        when(avroSourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);

        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(plainTextAuthConfig.getUsername()).thenReturn(username);
        when(plainTextAuthConfig.getPassword()).thenReturn(password);
        when(schemaConfig.getBasicAuthCredentialsSource()).thenReturn("USER_INFO");
        String[] userInfoParts = schemaRegistryUserInfo.split(":");
        when(schemaConfig.getSchemaRegistryApiKey()).thenReturn(userInfoParts[0]);
        when(schemaConfig.getSchemaRegistryApiSecret()).thenReturn(userInfoParts[1]);
        when(schemaConfig.getRegistryURL()).thenReturn(schemaRegistryUrl);
    }

    @Test
    public void KafkaJsonProducerConsumerTestWithSpecifiedSchemaVersion() {
        when(schemaConfig.getVersion()).thenReturn(2);
        produceJsonRecords(bootstrapServers,  numRecordsProduced);
        consumeRecords(bootstrapServers, jsonSourceConfig);
        await().atMost(Duration.ofSeconds(20)).
                untilAsserted(() -> assertThat(numRecordsReceived.get(), equalTo(numRecordsProduced)));
    }

    @Test
    public void KafkaJsonProducerConsumerTestWithLatestSchemaVersion() {
        when(schemaConfig.getVersion()).thenReturn(null);
        produceJsonRecords(bootstrapServers,  numRecordsProduced);
        consumeRecords(bootstrapServers, jsonSourceConfig);
        await().atMost(Duration.ofSeconds(20)).
                untilAsserted(() -> assertThat(numRecordsReceived.get(), equalTo(numRecordsProduced)));
    }

    public void consumeRecords(String servers, KafkaSourceConfig sourceConfig) {
        kafkaSource = new KafkaSource(
                sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription,
                null, pluginConfigObservable, awsCredentialsSupplier);
        kafkaSource.start(buffer);
    }

    public void produceJsonRecords(String servers, int numRecords) throws SerializationException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("request.timeout.ms", 20000);
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+username+"\" password=\""+password+"\";");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.url", schemaRegistryUrl);
        properties.put("basic.auth.user.info", schemaRegistryUserInfo);

        KafkaProducer<String, UserRecord> producer = new KafkaProducer<String, UserRecord>(properties);
        for (int i = 0; i < numRecords; i++) {
            String key = "key"+String.valueOf(i);
            UserRecord userRecord = new UserRecord(testMessage+i, testId+i, testValue*(i+1));
            ProducerRecord<String, UserRecord> record = new ProducerRecord<String, UserRecord>(jsonTopicName, key, userRecord);
            producer.send(record);
        }
        producer.flush();

    }


    @Test
    public void KafkaAvroProducerConsumerTestWithSpecifiedSchemaVersion() {
        when(schemaConfig.getVersion()).thenReturn(1);
	produceAvroRecords(bootstrapServers,  numRecordsProduced);
	consumeRecords(bootstrapServers, avroSourceConfig);
	await().atMost(Duration.ofSeconds(20)).
	  untilAsserted(() -> assertThat(numRecordsReceived.get(), equalTo(numRecordsProduced)));
    }

    @Test
    public void KafkaAvroProducerConsumerTestWithLatestSchemaVersion() {
        when(schemaConfig.getVersion()).thenReturn(null);
        produceAvroRecords(bootstrapServers,  numRecordsProduced);
        consumeRecords(bootstrapServers, avroSourceConfig);
        await().atMost(Duration.ofSeconds(20)).
                untilAsserted(() -> assertThat(numRecordsReceived.get(), equalTo(numRecordsProduced)));
    }

    public void produceAvroRecords(String servers, int numRecords) throws SerializationException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("request.timeout.ms", 20000);
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+username+"\" password=\""+password+"\";");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.url", schemaRegistryUrl);
        properties.put("basic.auth.user.info", schemaRegistryUserInfo);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties);
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"sampleAvroRecord\"," +
                "\"fields\":[{\"name\":\"message\",\"type\":\"string\"}, {\"name\":\"ident\",\"type\":\"int\"}, {\"name\":\"score\",\"type\":\"double\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        for (int i = 0; i < numRecords; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("message", testMessage+i);
            avroRecord.put("ident", testId+i);
            avroRecord.put("score", testValue*(i+1));
            String key = "key"+String.valueOf(i);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(avroTopicName, key, avroRecord);
            producer.send(record);
        }
        producer.flush();
    }

}
