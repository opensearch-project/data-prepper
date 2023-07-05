/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

/**
 * * A Multithreaded helper class which helps to produce the records to multiple topics in an
 * asynchronous way.
 */

public class ProducerWorker implements Runnable {

    private final Record<Event> record;
    private final KafkaSinkProducer producer;


    public ProducerWorker(final KafkaSinkProducer producer,
                          final Record<Event> record) {
        this.record = record;
        this.producer=producer;
    }

    @Override
    public void run() {
            producer.produceRecords(record);
    }

 }
