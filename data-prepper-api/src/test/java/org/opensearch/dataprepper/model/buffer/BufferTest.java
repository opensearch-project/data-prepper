/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

class BufferTest {

    private Buffer createObjectUnderTest() {
        return spy(Buffer.class);
    }

    @Test
    void testGetDrainTimeout() {
        final Buffer<Record<Event>> buffer = createObjectUnderTest();

        assertEquals(Duration.ZERO, buffer.getDrainTimeout());
    }

    @Test
    void testShutdown() {
        final Buffer<Record<Event>> buffer = createObjectUnderTest();
        buffer.shutdown();
    }

    @Test
    void testIsByteBuffer() {
        final Buffer<Record<Event>> buffer = createObjectUnderTest();

        assertEquals(false, buffer.isByteBuffer());
        assertEquals(false, buffer.areAcknowledgementsEnabled());
    }

    @Test
    void isWrittenOffHeapOnly_returns_false_by_default() {
        assertThat(createObjectUnderTest().isWrittenOffHeapOnly(), equalTo(false));
    }

    @Test
    void testWriteBytes() {
        final Buffer<Record<Event>> buffer = createObjectUnderTest();

        byte[] bytes = new byte[2];
        assertThrows(UnsupportedOperationException.class, () -> buffer.writeBytes(bytes, "", 10));

    }

}
