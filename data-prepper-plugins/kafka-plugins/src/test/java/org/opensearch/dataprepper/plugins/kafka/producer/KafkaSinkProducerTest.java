/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.event.DefaultEventHandle;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)

public class KafkaSinkProducerTest {

    private KafkaSinkProducer producer;

    @Mock
    private KafkaSinkConfig kafkaSinkConfig;

    private Record<Event> record;

    KafkaSinkProducer sinkProducer;

    @Mock
    private DLQSink dlqSink;

    private JacksonEvent event;

    @BeforeEach
    public void setUp() {
        event = (JacksonEvent) JacksonEvent.fromMessage(UUID.randomUUID().toString());
        DefaultEventHandle defaultEventHandle = mock(DefaultEventHandle.class);
        event.setEventHandle(defaultEventHandle);
        record = new Record<>(event);
        final TopicConfig topicConfig = new TopicConfig();
        topicConfig.setName("test-topic");

        when(kafkaSinkConfig.getTopic()).thenReturn(topicConfig);
        when(kafkaSinkConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
        when(kafkaSinkConfig.getSchemaConfig().getRegistryURL()).thenReturn("http://localhost:8085/");
        when(kafkaSinkConfig.getPartitionKey()).thenReturn("testkey");

    }

    @Test
    public void producePlainTextRecordsTest() throws ExecutionException, InterruptedException {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("plaintext");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig, dlqSink, mock(ExpressionEvaluator.class), null);
        sinkProducer = spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);
        assertEquals(1, mockProducer.history().size());

    }

    @Test
    public void produceJsonRecordsTest() throws RestClientException, IOException {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("JSON");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new JsonSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig, dlqSink, mock(ExpressionEvaluator.class), null);
        SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
        String jsonSchema = "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"message\": {\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "}\n" +
                "}\n";

        when(schemaMetadata.getSchema()).thenReturn(jsonSchema);
        sinkProducer = spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);
        assertEquals(1, mockProducer.history().size());
    }

    @Test
    public void produceAvroRecordsTest() throws Exception {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("AVRO");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig, dlqSink, mock(ExpressionEvaluator.class), null);
        SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
        String avroSchema = "{\"type\":\"record\",\"name\":\"MyMessage\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}";
        when(schemaMetadata.getSchema()).thenReturn(avroSchema);
        sinkProducer = spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);

    }

    @Test
    public void testGetGenericRecord() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        producer = new KafkaSinkProducer(new MockProducer(), kafkaSinkConfig, dlqSink, mock(ExpressionEvaluator.class), null);
        final Schema schema = createMockSchema();
        Method privateMethod = KafkaSinkProducer.class.getDeclaredMethod("getGenericRecord", Event.class, Schema.class);
        privateMethod.setAccessible(true);
        GenericRecord result = (GenericRecord) privateMethod.invoke(producer, event, schema);
        Assertions.assertNotNull(result);
    }

    private Schema createMockSchema() {
        String schemaDefinition = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}";
        return new Schema.Parser().parse(schemaDefinition);
    }


    @Test
    public void validateSchema() throws IOException, ProcessingException {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("avro");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig, dlqSink, mock(ExpressionEvaluator.class), null);
        String jsonSchema = "{\"type\": \"object\",\"properties\": {\"Year\": {\"type\": \"string\"},\"Age\": {\"type\": \"string\"},\"Ethnic\": {\"type\":\"string\",\"default\": null}}}";
        String jsonSchema2 = "{\"type\": \"object\",\"properties\": {\"Year\": {\"type\": \"string\"},\"Age\": {\"type\": \"string\"},\"Ethnic\": {\"type\":\"string\",\"default\": null}}}";
        assertTrue(producer.validateSchema(jsonSchema, jsonSchema2));
    }
}

