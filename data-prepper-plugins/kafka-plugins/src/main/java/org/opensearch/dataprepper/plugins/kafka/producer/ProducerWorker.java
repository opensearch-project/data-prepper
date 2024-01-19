/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A Multithreaded helper class which helps to produce the records to multiple topics in an
 * asynchronous way.
 */

public class ProducerWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerWorker.class);
    private final Record<Event> record;
    private final KafkaCustomProducer producer;


    public ProducerWorker(final KafkaCustomProducer producer,
                          final Record<Event> record) {
        this.record = record;
        this.producer = producer;
    }

    @Override
    public void run() {
        try {
            producer.produceRecords(record);
        } catch (Exception e) {
            LOG.error("The Kafka buffer failed to produce records to Kafka.", e);
        }
    }

}
