/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.kafka.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


class ProducerWorkerTest {

    @Mock
    ProducerWorker producerWorker;


    private Record<Event> record;


    @BeforeEach
    public void setUp() {
        Event event = JacksonEvent.fromMessage("Testing multithreaded producer");
        record = new Record<>(event);
    }


    private ProducerWorker createObjectUnderTest() {
        return new ProducerWorker(mock(KafkaCustomProducer.class), record);
    }

    @Test
    void testWritingToTopic() {
        producerWorker = createObjectUnderTest();
        Thread spySink = spy(new Thread(producerWorker));
        spySink.start();
        verify(spySink).start();
    }


}