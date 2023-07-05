/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;
import org.powermock.api.mockito.PowerMockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaSinkProducerTest {

    private KafkaSinkProducer producer;

    @Mock
    private KafkaSinkConfig kafkaSinkConfig;

    List<TopicConfig> topics = new ArrayList<TopicConfig>();


    private Record<Event> record;

    KafkaSinkProducer sinkProducer;

    @Mock
    private DLQSink dlqSink;

    private Event event;
    @BeforeEach
    public void setUp(){

        event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        record=new Record<>(event);
        final TopicConfig topicConfig = new TopicConfig();
        topicConfig.setName("test-topic");
        topics.add(topicConfig);

        when(kafkaSinkConfig.getTopics()).thenReturn(topics);
        when(kafkaSinkConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
        when(kafkaSinkConfig.getSchemaConfig().getRegistryURL()).thenReturn("http://localhost:8085/");

    }
    @Test
    public void producePlainTextRecordsTest() {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("plaintext");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig,dlqSink);
        sinkProducer = spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);

    }

    @Test
    public void produceJsonRecordsTest()  {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("json");
        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new JsonSerializer());
        producer=new KafkaSinkProducer(mockProducer, kafkaSinkConfig,dlqSink);
        sinkProducer=spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);

    }

    @Test
    public void produceAvroRecordsTest() throws Exception {
        when(kafkaSinkConfig.getSerdeFormat()).thenReturn("avro");
        CachedSchemaRegistryClient schemaRegistryClient=PowerMockito.mock(CachedSchemaRegistryClient.class);
        whenNew(CachedSchemaRegistryClient.class).withArguments(kafkaSinkConfig.getSchemaConfig().getRegistryURL(), 100)
                .thenReturn(schemaRegistryClient);

        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new KafkaAvroSerializer());
        producer = new KafkaSinkProducer(mockProducer, kafkaSinkConfig,dlqSink);
        sinkProducer = spy(producer);
        sinkProducer.produceRecords(record);
        verify(sinkProducer).produceRecords(record);

    }

    @Test
    public void testGetGenericRecord() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        producer=new KafkaSinkProducer(new MockProducer(), kafkaSinkConfig,dlqSink);
        final Schema schema = createMockSchema();
        Method privateMethod = KafkaSinkProducer.class.getDeclaredMethod("getGenericRecord", Event.class, Schema.class);
        privateMethod.setAccessible(true);
        GenericRecord result = (GenericRecord) privateMethod.invoke(producer, event,schema);
        Assertions.assertNotNull(result);
    }

    private Schema createMockSchema() {
        String schemaDefinition = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}";
        return new Schema.Parser().parse(schemaDefinition);
    }
}

