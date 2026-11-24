/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer.setAuthProperties;

public class ConfluentKafkaProducerConsumerWithSchemaRegistryIT {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentKafkaProducerConsumerWithSchemaRegistryIT.class);
    private static final int NUMBER_OF_RECORDS = 3;
    private static final long FIRST_COUNT = 9_007_199_254_740_993L;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @ParameterizedTest(name = "Avro round trip, latest schema = {0}")
    @ValueSource(booleans = {false, true})
    void avroRecords_preserveFieldsKeysAndOffsets(final boolean useLatestSchema) throws Exception {
        final Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
                + "{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}");

        assertRoundTrip(new AvroSchema(schema), new KafkaAvroSerializer(), index -> {
            final GenericRecord record = new GenericData.Record(schema);
            record.put("message", "message-" + index);
            record.put("count", FIRST_COUNT + index);
            return record;
        }, useLatestSchema);
    }

    @ParameterizedTest(name = "JSON Schema round trip, latest schema = {0}")
    @ValueSource(booleans = {false, true})
    void jsonSchemaRecords_preserveFieldsKeysAndOffsets(final boolean useLatestSchema) throws Exception {
        final JsonSchema schema = new JsonSchema("{\"type\":\"object\",\"properties\":{"
                + "\"message\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"}},"
                + "\"required\":[\"message\",\"count\"],\"additionalProperties\":false}");

        assertRoundTrip(schema, new KafkaJsonSchemaSerializer<JsonNode>(), index -> JsonSchemaUtils.envelope(schema,
                objectMapper.createObjectNode().put("message", "message-" + index).put("count", FIRST_COUNT + index)), useLatestSchema);
    }

    @ParameterizedTest(name = "Protobuf round trip, latest schema = {0}")
    @ValueSource(booleans = {false, true})
    void protobufRecords_preserveFieldsKeysAndNumericInt64AndOffsets(final boolean useLatestSchema) throws Exception {
        final Descriptors.Descriptor descriptor = new ProtobufSchema("syntax = \"proto3\"; package integration;"
                + " message TestRecord { string message = 1; int64 count = 2; }").toDescriptor();

        assertRoundTrip(new ProtobufSchema(descriptor), new KafkaProtobufSerializer<DynamicMessage>(), index -> DynamicMessage
                .newBuilder(descriptor)
                .setField(descriptor.findFieldByName("message"), "message-" + index)
                .setField(descriptor.findFieldByName("count"), FIRST_COUNT + index)
                .build(), useLatestSchema);
    }

    private <T> void assertRoundTrip(final ParsedSchema schema, final Serializer<T> serializer,
                                    final IntFunction<T> recordFactory, final boolean useLatestSchema) throws Exception {
        final String topic = "schema-registry-it-" + UUID.randomUUID();
        final String subject = topic + "-value";
        final KafkaSourceConfig sourceConfig = createSourceConfig(topic, useLatestSchema);
        final Properties properties = new Properties();
        final AwsCredentialsSupplier credentialsSupplier = mock(AwsCredentialsSupplier.class);
        setAuthProperties(properties, sourceConfig, credentialsSupplier, LOG);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
        properties.put(AUTO_REGISTER_SCHEMAS, false);

        final Map<String, Object> registryProperties = new HashMap<>();
        registryProperties.put("schema.registry.url", sourceConfig.getSchemaConfig().getRegistryURL());
        if (sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource() != null) {
            registryProperties.put("basic.auth.credentials.source", "USER_INFO");
            registryProperties.put("basic.auth.user.info", sourceConfig.getSchemaConfig().getSchemaRegistryApiKey()
                    + ":" + sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret());
        }
        properties.putAll(registryProperties);

        try (AdminClient admin = AdminClient.create(properties);
             CachedSchemaRegistryClient registry = new CachedSchemaRegistryClient(
                     sourceConfig.getSchemaConfig().getRegistryURL(), 100, registryProperties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            try {
                registry.register(subject, schema);
                try {
                    assertThat(registry.getSchemaMetadata(subject, 1).getSchemaType(), equalTo(schema.schemaType()));
                    final Map<String, Object> serializerProperties = new HashMap<>(registryProperties);
                    serializerProperties.put(VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
                    serializerProperties.put(AUTO_REGISTER_SCHEMAS, false);
                    serializer.configure(serializerProperties, false);
                    try (KafkaProducer<String, T> producer = new KafkaProducer<>(properties, new StringSerializer(), serializer)) {
                        for (int index = 0; index < NUMBER_OF_RECORDS; index++) {
                            producer.send(new ProducerRecord<>(topic, "key-" + index, recordFactory.apply(index)))
                                    .get(30, TimeUnit.SECONDS);
                        }
                    }
                    consumeAndVerify(sourceConfig, admin, credentialsSupplier);
                } finally {
                    registry.deleteSubject(subject);
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private void consumeAndVerify(final KafkaSourceConfig sourceConfig, final AdminClient admin,
                                  final AwsCredentialsSupplier credentialsSupplier) throws Exception {
        final List<Record<Event>> receivedRecords = new CopyOnWriteArrayList<>();
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        doAnswer(invocation -> {
            final Collection<Record<Event>> records = invocation.getArgument(0);
            receivedRecords.addAll(records);
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());
        final PluginMetrics metrics = mock(PluginMetrics.class);
        when(metrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(metrics.timer(anyString())).thenReturn(mock(Timer.class));
        final PipelineDescription pipeline = mock(PipelineDescription.class);
        final String topic = sourceConfig.getTopics().get(0).getName();
        when(pipeline.getPipelineName()).thenReturn(topic);
        final KafkaSource source = new KafkaSource(sourceConfig, metrics, mock(AcknowledgementSetManager.class), pipeline,
                null, mock(PluginConfigObservable.class), credentialsSupplier);
        try {
            source.start(buffer);
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                assertThat(receivedRecords, hasSize(NUMBER_OF_RECORDS));
                for (int index = 0; index < NUMBER_OF_RECORDS; index++) {
                    final Event event = receivedRecords.get(index).getData();
                    assertThat(event.get("message", String.class), equalTo("message-" + index));
                    assertThat(event.get("count", Object.class), instanceOf(Number.class));
                    assertThat(event.get("count", Number.class).longValue(), equalTo(FIRST_COUNT + index));
                    assertThat(event.getMetadata().getAttribute("kafka_key"), equalTo("key-" + index));
                    assertThat(event.getMetadata().getAttribute("kafka_topic"), equalTo(topic));
                }
                final Map<TopicPartition, OffsetAndMetadata> offsets = admin
                        .listConsumerGroupOffsets(sourceConfig.getTopics().get(0).getGroupId())
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                assertThat(offsets.containsKey(new TopicPartition(topic, 0)), equalTo(true));
                assertThat(offsets.get(new TopicPartition(topic, 0)).offset(), equalTo((long) NUMBER_OF_RECORDS));
            });
        } finally {
            source.stop();
        }
    }

    private KafkaSourceConfig createSourceConfig(final String topic, final boolean useLatestSchema) {
        final String bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");
        final String registryUrl = System.getProperty("tests.kafka.schema_registry_url");
        assertThat("Set tests.kafka.bootstrap_servers", bootstrapServers, not(emptyOrNullString()));
        assertThat("Set tests.kafka.schema_registry_url", registryUrl, not(emptyOrNullString()));
        final Map<String, Object> schema = new HashMap<>();
        schema.put("type", "confluent");
        schema.put("registry_url", registryUrl);
        if (!useLatestSchema) {
            schema.put("version", 1);
        }
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put("bootstrap_servers", List.of(bootstrapServers));
        configuration.put("schema", schema);
        configuration.put("topics", List.of(Map.of(
                "name", topic, "group_id", topic + "-group", "workers", 1,
                "key_mode", "include_as_metadata", "auto_offset_reset", "earliest", "auto_commit", false,
                "thread_waiting_time", "PT1S", "commit_interval", "PT1S")));

        final String username = System.getProperty("tests.kafka.username", "");
        final String password = System.getProperty("tests.kafka.password", "");
        assertThat("Kafka username and password must be supplied together", username.isEmpty(), equalTo(password.isEmpty()));
        configuration.put("encryption", Map.of("type", username.isEmpty() ? "none" : "ssl"));
        if (!username.isEmpty()) {
            configuration.put("authentication", Map.of("sasl", Map.of("plaintext", Map.of("username", username, "password", password))));
        }
        final String registryUserInfo = System.getProperty("tests.kafka.schema_registry_userinfo", "");
        if (!registryUserInfo.isEmpty()) {
            assertThat("Registry authentication requires Kafka SASL credentials", username, not(emptyOrNullString()));
            final String[] credentials = registryUserInfo.split(":", 2);
            assertThat("Registry credentials must be user:password", credentials.length, equalTo(2));
            schema.put("basic_auth_credentials_source", "USER_INFO");
            schema.put("api_key", credentials[0]);
            schema.put("api_secret", credentials[1]);
        }
        return objectMapper.convertValue(configuration, KafkaSourceConfig.class);
    }
}
