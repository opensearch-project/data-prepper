package org.opensearch.dataprepper.plugins.kafka.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;

import static org.mockito.Mockito.*;


class ProducerWorkerTest {

    @Mock
    ProducerWorker multithreadedProducer;

    @Mock
    KafkaSinkConfig kafkaSinkConfig;

    private Record<Event> record;


    @BeforeEach
    public void setUp(){
        Event event= JacksonEvent.fromMessage("Testing multithreaded producer");
        record=new Record<>(event);
    }


    private ProducerWorker createObjectUnderTest(){
           return new ProducerWorker(mock(KafkaSinkProducer.class),record);
    }

    @Test
    void testWritingToTopic()  {
        multithreadedProducer = createObjectUnderTest();
        Thread spySink = spy(new Thread(multithreadedProducer));
        spySink.start();
        verify(spySink).start();
    }





}