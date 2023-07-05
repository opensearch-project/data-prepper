/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;


/**
 * * A helper class which helps takes the buffer data
 * and produce it to a given kafka topic
 */

public class KafkaSinkProducer<T> {

    private final Producer<String, T> producer;

    private final KafkaSinkConfig kafkaSinkConfig;

    private final DLQSink dlqSink;

    private final CachedSchemaRegistryClient schemaRegistryClient;


    public KafkaSinkProducer(final Producer producer,
                             final KafkaSinkConfig kafkaSinkConfig,
                             final DLQSink dlqSink) {
        this.producer = producer;
        this.kafkaSinkConfig = kafkaSinkConfig;
        this.dlqSink=dlqSink;;
        schemaRegistryClient=getSchemaRegistryClient();
    }


    public void produceRecords(final Record<Event> record) {
        kafkaSinkConfig.getTopics().forEach(topic -> {
            Object dataForDlq = null;
            try {
                String serdeFormat=kafkaSinkConfig.getSerdeFormat();
                if (MessageFormat.JSON.toString().equalsIgnoreCase(serdeFormat)) {
                    JsonNode dataNode = new ObjectMapper().convertValue(record.getData().toJsonString(), JsonNode.class);
                    dataForDlq = dataNode;
                    producer.send(new ProducerRecord(topic.getName(), dataNode));
                }
                else if (MessageFormat.AVRO.toString().equalsIgnoreCase(serdeFormat)) {
                    final String valueToParse = schemaRegistryClient.
                            getLatestSchemaMetadata(topic.getName() + "-value").getSchema();
                    Schema schema =new Schema.Parser().parse(valueToParse);
                    GenericRecord genericRecord = getGenericRecord(record.getData(),schema);
                    dataForDlq = genericRecord;
                    producer.send(new ProducerRecord(topic.getName(), genericRecord));
                } else {
                    dataForDlq = record.getData().toJsonString();
                    producer.send(new ProducerRecord(topic.getName(), record.getData().toJsonString()));
                }
            } catch (Exception e) {
               dlqSink.perform(dataForDlq);
            }
        });


    }

    private CachedSchemaRegistryClient getSchemaRegistryClient(){
        return new CachedSchemaRegistryClient(kafkaSinkConfig.getSchemaConfig().getRegistryURL(),
                100);
    }

    //TODO: we need feedback if this is suffient or there will be complex objects that will need conversion.
    //TODO: This code is coming from sink-codec. Because below code was part of a private method in class
    private GenericRecord getGenericRecord(Event event, Schema schema) {
        final GenericRecord record = new GenericData.Record(schema);
        for (final String key : event.toMap().keySet()) {
            record.put(key, event.toMap().get(key));
        }
        return record;
    }


}
