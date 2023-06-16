/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.List;


/**
 * * A helper class which helps takes the buffer data
 * and produce it to a given kafka topic
 */

@SuppressWarnings("deprecation")
public class KafkaSinkProducer {

    final KafkaProducer<String, Object> producer;

    final KafkaSinkConfig kafkaSinkConfig;

    final Record<Event> record;

    final String schemaType;

    public KafkaSinkProducer(final KafkaProducer producer,
                             final Record<Event> record,
                             final KafkaSinkConfig kafkaSinkConfig,
                             final String schemaType) {
        this.producer = producer;
        this.record = record;
        this.kafkaSinkConfig = kafkaSinkConfig;
        this.schemaType = schemaType;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void produceRecords() {
        if (MessageFormat.JSON.toString().equalsIgnoreCase(schemaType)) {
            JsonNode dataNode = new ObjectMapper().convertValue(record.getData().toJsonString(), JsonNode.class);
            sendToTopics(kafkaSinkConfig.getTopics(),  dataNode);
        }
        //TODO: We are not testing AVRO without schema registry, hence this code needs to be changed
        else if (MessageFormat.AVRO.toString().equalsIgnoreCase(schemaType)) {
            sendToTopics(kafkaSinkConfig.getTopics(),  record.getData());
        }
        else {
            sendToTopics(kafkaSinkConfig.getTopics(),  record.getData().toJsonString());
        }
    }

    private void sendToTopics(List<TopicConfig> topics, Object data) {
        try {
            topics.forEach(topic -> {
                producer.send(new ProducerRecord(topic.getName(), data));
            });
        }
        catch(Exception e){
            //TODO: handle retries and DLQ
            e.printStackTrace();
        }

    }


}
