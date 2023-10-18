/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;

import static org.mockito.Mockito.spy;

public class BufferTest {

    @Test
    public void testGetDrainTimeout() {
        final Buffer<Record<Event>> buffer = spy(Buffer.class);

        Assert.assertEquals(Duration.ZERO, buffer.getDrainTimeout());
    }

    @Test
    public void testShutdown() {
        final Buffer<Record<Event>> buffer = spy(Buffer.class);
        buffer.shutdown();
    }

    @Test
    public void testIsByteBuffer() {
        final Buffer<Record<Event>> buffer = spy(Buffer.class);

        Assert.assertEquals(false, buffer.isByteBuffer());
    }

    @Test
    public void testWriteBytes() {
        final Buffer<Record<Event>> buffer = spy(Buffer.class);

        byte[] bytes = new byte[2];
        Assert.assertThrows(RuntimeException.class, () -> buffer.writeBytes(bytes, "", 10));

    }

}
