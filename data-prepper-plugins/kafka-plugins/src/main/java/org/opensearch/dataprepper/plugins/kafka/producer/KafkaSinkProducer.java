/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;


/**
 * * A helper class which helps takes the buffer data
 * and produce it to a given kafka topic
 */

public class KafkaSinkProducer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkProducer.class);

    private final Producer<String, T> producer;

    private final KafkaSinkConfig kafkaSinkConfig;

    private final DLQSink dlqSink;

    private final CachedSchemaRegistryClient schemaRegistryClient;

    private final Collection<EventHandle> bufferedEventHandles;

    private final ExpressionEvaluator expressionEvaluator;

    private final String tagTargetKey;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaSinkProducer(final Producer producer,
                             final KafkaSinkConfig kafkaSinkConfig,
                             final DLQSink dlqSink,
                             final CachedSchemaRegistryClient schemaRegistryClient,
                             final ExpressionEvaluator expressionEvaluator,
                             final String tagTargetKey) {
        this.producer = producer;
        this.kafkaSinkConfig = kafkaSinkConfig;
        this.dlqSink = dlqSink;
        this.schemaRegistryClient = schemaRegistryClient;
        bufferedEventHandles = new LinkedList<>();
        this.expressionEvaluator = expressionEvaluator;
        this.tagTargetKey = tagTargetKey;

    }

    public void produceRecords(final Record<Event> record) {
        if (record.getData().getEventHandle() != null) {
            bufferedEventHandles.add(record.getData().getEventHandle());
        }
        TopicConfig topic = kafkaSinkConfig.getTopic();
        Event event = getEvent(record);
        final String key = event.formatString(kafkaSinkConfig.getPartitionKey(), expressionEvaluator);
        Object dataForDlq = event.toJsonString();
        LOG.info("Producing record " + dataForDlq);
        try {
            final String serdeFormat = kafkaSinkConfig.getSerdeFormat();
            if (MessageFormat.JSON.toString().equalsIgnoreCase(serdeFormat)) {
                publishJsonMessage(record, topic, key, dataForDlq);
            } else if (MessageFormat.AVRO.toString().equalsIgnoreCase(serdeFormat)) {
                publishAvroMessage(record, topic, key, dataForDlq);
            } else {
                publishPlaintextMessage(record, topic, key, dataForDlq);
            }
        } catch (Exception e) {
            releaseEventHandles(false);
        }

    }

    private Event getEvent(Record<Event> record) {
        Event event = record.getData();
        try {
            event = addTagsToEvent(event, tagTargetKey);
        } catch (JsonProcessingException e) {
            LOG.error("error occured while processing tag target key");
        }
        return event;
    }

    private void publishPlaintextMessage(Record<Event> record, TopicConfig topic, String key, Object dataForDlq) {
        producer.send(new ProducerRecord(topic.getName(), key, record.getData().toJsonString()), callBack(dataForDlq));
    }

    private void publishAvroMessage(Record<Event> record, TopicConfig topic, String key, Object dataForDlq) throws RestClientException, IOException {
        final String valueToParse = schemaRegistryClient.
                getLatestSchemaMetadata(topic.getName() + "-value").getSchema();
        final Schema schema = new Schema.Parser().parse(valueToParse);
        final GenericRecord genericRecord = getGenericRecord(record.getData(), schema);
        producer.send(new ProducerRecord(topic.getName(), key, genericRecord), callBack(dataForDlq));
    }

    private void publishJsonMessage(Record<Event> record, TopicConfig topic, String key, Object dataForDlq) throws IOException, RestClientException, ProcessingException {
        final JsonNode dataNode = new ObjectMapper().convertValue(record.getData().toJsonString(), JsonNode.class);
        if (validateJson(topic.getName(), dataForDlq)) {
            producer.send(new ProducerRecord(topic.getName(), key, dataNode), callBack(dataForDlq));
        } else {
            dlqSink.perform(dataForDlq, new RuntimeException("Invalid Json"));
        }
    }

    private Boolean validateJson(final String topicName, Object dataForDlq) throws IOException, RestClientException, ProcessingException {
        if (schemaRegistryClient != null) {
            final String schemaJson = schemaRegistryClient.
                    getLatestSchemaMetadata(topicName + "-value").getSchema();
            return validateSchema(dataForDlq.toString(), schemaJson);
        } else {
            return true;
        }
    }

    private boolean validateSchema(final String jsonData, final String schemaJson) throws IOException, ProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode schemaNode = objectMapper.readTree(schemaJson);
        JsonNode dataNode = objectMapper.readTree(jsonData);
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.byDefault();
        JsonSchema schema = schemaFactory.getJsonSchema(schemaNode);
        ProcessingReport report = schema.validate(dataNode);
        if (report.isSuccess()) {
            return true;
        } else {
            return false;
        }
    }

    private Callback callBack(final Object dataForDlq) {
        return (metadata, exception) -> {
            if (null != exception) {
                releaseEventHandles(false);
                dlqSink.perform(dataForDlq, exception);
            } else {
                releaseEventHandles(true);
            }
        };
    }


    private GenericRecord getGenericRecord(final Event event, final Schema schema) {
        final GenericRecord record = new GenericData.Record(schema);
        for (final String key : event.toMap().keySet()) {
            record.put(key, event.toMap().get(key));
        }
        return record;
    }

    private void releaseEventHandles(final boolean result) {
        for (final EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    private Event addTagsToEvent(Event event, String tagsTargetKey) throws JsonProcessingException {
        String eventJsonString = event.jsonBuilder().includeTags(tagsTargetKey).toJsonString();
        Map<String, Object> eventData = objectMapper.readValue(eventJsonString, new TypeReference<>() {
        });
        return JacksonLog.builder().withData(eventData).build();
    }

}
