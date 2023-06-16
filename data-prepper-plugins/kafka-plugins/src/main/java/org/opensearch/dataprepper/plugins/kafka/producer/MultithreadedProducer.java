/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A Multithreaded helper class which helps to produce the records to multiple topics in an
 * asynchronous way.
 */
@SuppressWarnings("deprecation")
public class MultithreadedProducer implements Runnable {
    private KafkaProducer<String, String> plainTextProducer = null;
    private KafkaProducer<String, JsonNode> jsonProducer = null;
    private KafkaProducer<String, GenericRecord> avroProducer = null;
    private static final Logger LOG = LoggerFactory.getLogger(MultithreadedProducer.class);
    private final AtomicBoolean status = new AtomicBoolean(false);
    private final KafkaSinkConfig sinkConfig;

    private final Record<Event> record;

    private String schemaType;
    private Properties producerProperties;
    private PluginMetrics pluginMetrics;

    public MultithreadedProducer(Properties properties,
                                 KafkaSinkConfig sinkConfig, Record<Event> record,
                                 PluginMetrics pluginMetric,
                                 String schemaType) {
        this.producerProperties = Objects.requireNonNull(properties);
        this.sinkConfig = sinkConfig;
        this.record = record;
        this.schemaType = schemaType;
        this.pluginMetrics = pluginMetric;
        this.jsonProducer = new KafkaProducer<>(producerProperties);
        this.plainTextProducer = new KafkaProducer<>(producerProperties);
        this.avroProducer = new KafkaProducer<>(producerProperties);

    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void run() {
        try {
            MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);
            switch (schema) {
                case JSON:
                    new KafkaSinkProducer(jsonProducer, record, sinkConfig, schemaType).produceRecords();
                    break;
                case AVRO:
                    new KafkaSinkProducer(avroProducer, record, sinkConfig, schemaType).produceRecords();
                    break;
                case PLAINTEXT:
                default:
                    new KafkaSinkProducer(plainTextProducer, record, sinkConfig, schemaType).produceRecords();
                    break;
            }

        } catch (Exception exp) {
            if (exp.getCause() instanceof WakeupException && !status.get()) {
                LOG.error("Error writing records from the topic...{}", exp.getMessage());
            }
        } finally {
            closeProducers();
        }
    }

    private void closeProducers() {
        if (plainTextProducer != null) {
            plainTextProducer.close();
            plainTextProducer = null;
        }
        if (jsonProducer != null) {
            jsonProducer.close();
            jsonProducer = null;
        }
        if (avroProducer != null) {
            avroProducer.close();
            avroProducer = null;
        }
    }


}
