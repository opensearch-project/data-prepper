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
}
