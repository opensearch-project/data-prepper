/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PeerForwardingProcessingDecoratorTest {
    private Record<Event> record;

    @Mock
    Processor processorMock;

    @Mock
    PeerForwarder peerForwarder;

    @BeforeEach
    void setUp() {
        record = mock(Record.class);
    }

    private PeerForwardingProcessorDecorator createObjectUnderTest() {
        return new PeerForwardingProcessorDecorator(processorMock, peerForwarder);
    }

    @Test
    void PeerForwardingProcessingDecorator_execute_will_call_inner_processors_execute() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        Collection<Record<Event>> testData = Collections.singletonList(record);
        objectUnderTest.execute(testData);
        verify(processorMock).execute(any(Collection.class));
    }

    @Test
    void PeerForwardingProcessingDecorator_prepareForShutdown_will_call_inner_processors_prepareForShutdown() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        objectUnderTest.prepareForShutdown();
        verify(processorMock).prepareForShutdown();
    }

    @Test
    void PeerForwardingProcessingDecorator_isReadyForShutdown_will_call_inner_processors_isReadyForShutdown() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        objectUnderTest.isReadyForShutdown();
        verify(processorMock).isReadyForShutdown();
    }

    @Test
    void PeerForwardingProcessingDecorator_shutdown_will_call_inner_processors_shutdown() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        objectUnderTest.shutdown();
        verify(processorMock).shutdown();
    }

}