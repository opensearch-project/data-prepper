/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import io.micrometer.core.instrument.Counter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.MskBrokerConnectionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.File;
import java.io.IOException;
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

public class MskGlueRegistryMultiTypeIT {
    private static final String TEST_USER = "user";
    private static final String TEST_MESSAGE = "test message ";
    private static final Long TEST_TIMESTAMP = 1366154481L;
    private static final Integer TEST_TIMESTAMP_INT = 12345;
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
    private TopicConfig plainTextTopic;

    @Mock
    private AuthConfig authConfig;

    private AwsConfig awsConfig;

    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    private AwsConfig.AwsMskConfig awsMskConfig;

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsCredentialsOptions awsCredentialsOptions;

    private KafkaSource kafkaSource;
    private SourceTopicConfig jsonTopic;
    private SourceTopicConfig avroTopic;

    private Counter counter;

    private List<Record> receivedRecords;

    private String bootstrapServers;

    private String testRegistryName;

    private String testAvroSchemaName;

    private String testJsonSchemaName;

    private String testMskArn;

    private String testMskRegion;

    @Mock
    SchemaConfig schemaConfig;


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
        awsConfig = mock(AwsConfig.class);
        awsMskConfig = mock(AwsConfig.AwsMskConfig.class);
        authConfig = mock(AuthConfig.class);
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        schemaConfig = mock(SchemaConfig.class);
        receivedRecords = new ArrayList<>();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsOptions = mock(AwsCredentialsOptions.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(sourceConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(schemaConfig.getType()).thenReturn(SchemaRegistryType.AWS_GLUE);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(pipelineDescription.getPipelineName()).thenReturn("testPipeline");
        try {
            doAnswer(args -> {
                Collection<Record<Event>> bufferedRecords = (Collection<Record<Event>>)args.getArgument(0);
                receivedRecords.addAll(bufferedRecords);
                Record<Event> r = receivedRecords.get(0);
                return null;
            }).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        } catch (Exception e){}

        final String testGroup = "TestGroup_"+RandomStringUtils.randomAlphabetic(6);
        final String testTopic = "TestTopic_"+RandomStringUtils.randomAlphabetic(5);
        avroTopic = mock(SourceTopicConfig.class);
        jsonTopic = mock(SourceTopicConfig.class);
        when(avroTopic.getName()).thenReturn(testTopic);
        when(avroTopic.getGroupId()).thenReturn(testGroup);
        when(avroTopic.getWorkers()).thenReturn(1);
        when(avroTopic.getAutoCommit()).thenReturn(false);
        when(avroTopic.getAutoOffsetReset()).thenReturn("earliest");
        when(avroTopic.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(avroTopic.getMaxPollInterval()).thenReturn(Duration.ofSeconds(300));
        when(avroTopic.getSessionTimeOut()).thenReturn(Duration.ofSeconds(10));
        when(avroTopic.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(avroTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        when(jsonTopic.getName()).thenReturn(testTopic);
        when(jsonTopic.getMaxPollInterval()).thenReturn(Duration.ofSeconds(300));
        when(jsonTopic.getGroupId()).thenReturn(testGroup);
        when(jsonTopic.getWorkers()).thenReturn(1);
        when(jsonTopic.getAutoCommit()).thenReturn(false);
        when(jsonTopic.getAutoOffsetReset()).thenReturn("earliest");
        when(jsonTopic.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(jsonTopic.getSessionTimeOut()).thenReturn(Duration.ofSeconds(10));
        when(jsonTopic.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        when(jsonTopic.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        testRegistryName = System.getProperty("tests.kafka.glue_registry_name");
        testJsonSchemaName = System.getProperty("tests.kafka.glue_json_schema_name");
        testAvroSchemaName = System.getProperty("tests.kafka.glue_avro_schema_name");
        testMskArn = System.getProperty("tests.msk.arn");
        testMskRegion = System.getProperty("tests.msk.region");
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        encryptionConfig = mock(EncryptionConfig.class);
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        when(awsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptions)).thenReturn(DefaultCredentialsProvider.create());
    }

    @Test
    public void TestJsonRecordConsumer() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(jsonTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getTopics()).thenReturn((List) List.of(jsonTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getAwsIamAuthConfig()).thenReturn(AwsIamAuthConfig.DEFAULT);
        when(sourceConfig.getAwsConfig()).thenReturn(awsConfig);
        when(awsConfig.getRegion()).thenReturn(testMskRegion);
        when(awsConfig.getAwsMskConfig()).thenReturn(awsMskConfig);
        when(awsMskConfig.getArn()).thenReturn(testMskArn);
        when(awsMskConfig.getBrokerConnectionType()).thenReturn(MskBrokerConnectionType.PUBLIC);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
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
            assertThat(event.get("username", String.class), equalTo(TEST_USER+i));
            assertThat(event.get("message", String.class), equalTo(TEST_MESSAGE+i));
            assertThat(event.get("timestamp", Number.class).intValue(), equalTo(TEST_TIMESTAMP_INT+i));
            assertThat(event.get("kafka_key", String.class), equalTo("message-"+i));
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
    public void TestAvroRecordConsumer() throws Exception {
        final int numRecords = 1;
        when(encryptionConfig.getType()).thenReturn(EncryptionType.SSL);
        when(avroTopic.getConsumerMaxPollRecords()).thenReturn(numRecords);
        when(sourceConfig.getTopics()).thenReturn((List) List.of(avroTopic));
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getAwsIamAuthConfig()).thenReturn(AwsIamAuthConfig.DEFAULT);
        when(sourceConfig.getAwsConfig()).thenReturn(awsConfig);
        when(awsConfig.getRegion()).thenReturn(testMskRegion);
        when(awsConfig.getAwsMskConfig()).thenReturn(awsMskConfig);
        when(awsMskConfig.getArn()).thenReturn(testMskArn);
        when(awsMskConfig.getBrokerConnectionType()).thenReturn(MskBrokerConnectionType.PUBLIC);
        kafkaSource = createObjectUnderTest();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        AtomicBoolean created = new AtomicBoolean(false);
        final String topicName = avroTopic.getName();
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
        produceAvroRecords(bootstrapServers, topicName, numRecords);
        int numRetries = 0;
        while (numRetries++ < 10 && (receivedRecords.size() != numRecords)) {
            Thread.sleep(1000);
        }
        assertThat(receivedRecords.size(), equalTo(numRecords));
	for (int i = 0; i < numRecords; i++) {
            Record<Event> record = receivedRecords.get(i);
            Event event = (Event)record.getData();
            assertThat(event.get("username", String.class), equalTo(TEST_USER+i));
            assertThat(event.get("message", String.class), equalTo(TEST_MESSAGE+i));
            assertThat(event.get("timestamp", Number.class).longValue(), equalTo(TEST_TIMESTAMP+i));
            assertThat(event.get("kafka_key", String.class), equalTo(TEST_USER+i));
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

    public void produceJsonRecords(final String servers, final String topic, int numRecords) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put("region", testMskRegion);
        properties.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, "json");
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, awsConfig.getRegion());
        properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, testRegistryName);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, testJsonSchemaName);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true); // If not passed, defaults to false

	String jsonSchema = "{\n"+
	    "\"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"+
	    "\"type\": \"object\",\n"+
	    "\"properties\": {\n"+
		"\"username\": {\n"+
		    "\"type\": \"string\",\n"+
		    "\"description\": \"user name.\"\n"+
                "},\n"+
		"\"message\": {\n"+
		    "\"type\": \"string\",\n"+
		    "\"description\": \"message.\"\n"+
                "},\n"+
		"\"timestamp\": {\n"+
		    "\"type\": \"integer\",\n"+
		    "\"description\": \"timestamp\"\n"+
                "}\n"+
	    "}\n"+
        "}";
	try (KafkaProducer<String, JsonDataWithSchema> producer = new KafkaProducer<String, JsonDataWithSchema>(properties)) {
		for (int i = 0; i < numRecords; i++) {
		    String jsonPayLoad = "{\n" +
				     "    \"username\": \""+TEST_USER+i+"\",\n" +
				     "    \"message\": \""+TEST_MESSAGE+i+"\",\n"+
				     "    \"timestamp\": "+(TEST_TIMESTAMP_INT+i)+"\n"+
				     "}";
		    JsonDataWithSchema testRecord = JsonDataWithSchema.builder(jsonSchema, jsonPayLoad).build();
		    final ProducerRecord<String, JsonDataWithSchema> record;
		    String topicKey = new String("message-"+i);
                    record = new ProducerRecord<String, JsonDataWithSchema>(topic, topicKey, testRecord);
                    producer.send(record);
                    Thread.sleep(1000L);
		}
	        producer.flush();
        }
    }


    public void produceAvroRecords(String servers, String topic, int numRecords) throws InterruptedException, IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put("region", testMskRegion);
        properties.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, "avro");
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, awsConfig.getRegion());
        properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, testRegistryName);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, testAvroSchemaName);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        Schema testSchema = null;
        Schema.Parser parser = new Schema.Parser();
        testSchema = parser.parse(new File("src/integrationTest/resources/test.avsc"));
        
        List<GenericRecord> testRecords = new ArrayList<>();
	for (int i = 0; i < numRecords; i++) {
            GenericRecord testRecord = new GenericData.Record(testSchema);
            testRecord.put("username", TEST_USER+i);
            testRecord.put("message", TEST_MESSAGE+i);
            testRecord.put("timestamp", TEST_TIMESTAMP+i);
            testRecords.add(testRecord);
        }

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties)) {
            for (int i = 0; i < testRecords.size(); i++) {
                GenericRecord r = testRecords.get(i);

                final ProducerRecord<String, GenericRecord> record;
                record = new ProducerRecord<String, GenericRecord>(topic, r.get("username").toString(), r);

                producer.send(record);
                Thread.sleep(1000L);
            }
            producer.flush();

        }
    }

}
