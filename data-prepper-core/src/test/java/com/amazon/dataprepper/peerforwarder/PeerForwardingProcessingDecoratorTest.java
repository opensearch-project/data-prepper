/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class PeerForwardingProcessingDecoratorTest {
    private Record<Event> record;

    @BeforeEach
    void setUp() {
        record = mock(Record.class);
    }

    private PeerForwardingProcessorDecorator createObjectUnderTest() {
        return new PeerForwardingProcessorDecorator();
    }

    @Test
    void PeerForwardingProcessingDecorator_will_throw_NotImplemented_exception() {
        PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest();
        Collection<Record<Event>> testData = Collections.singletonList(record);
        assertThrows(NotImplementedException.class, () -> objectUnderTest.execute(testData));
    }

}