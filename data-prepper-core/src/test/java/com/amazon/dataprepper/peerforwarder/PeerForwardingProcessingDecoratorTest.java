/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class PeerForwardingProcessingDecoratorTest {
    private Record<Event> record;

    @Mock
    Processor processorMock;

    @BeforeEach
    void setUp() {
        record = mock(Record.class);
    }

    private PeerForwardingProcessorDecorator createObjectUnderTest() {
        return new PeerForwardingProcessorDecorator(processorMock);
    }

    @Test
    void PeerForwardingProcessingDecorator_will_throw_NotImplemented_exception() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        Collection<Record<Event>> testData = Collections.singletonList(record);
        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.execute(testData));
    }

}