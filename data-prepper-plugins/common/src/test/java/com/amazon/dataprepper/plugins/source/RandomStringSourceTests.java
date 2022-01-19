/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.TestBuffer;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class RandomStringSourceTests {

    @Test
    public void testPutRecord() throws InterruptedException {
        final RandomStringSource randomStringSource =
                new RandomStringSource();
        final Queue<Record<Event>> bufferQueue = new ConcurrentLinkedQueue<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        //Start source, and sleep for 1000 millis
        randomStringSource.start(buffer);
        Thread.sleep(1000);
        //Stop the source, and wait long enough that another message would be sent
        //if the source was running
        assertThat(buffer.size(), greaterThan(0));
        Thread.sleep(1000);
        randomStringSource.stop();
        assertThat(buffer.size(), greaterThan(0));
    }
}
