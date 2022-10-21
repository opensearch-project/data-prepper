/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.TestBuffer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

class RandomStringSourceTests {

    private TestBuffer buffer;

    @BeforeEach
    void setUp() {
        final Queue<Record<Event>> bufferQueue = new ConcurrentLinkedQueue<>();
        buffer = new TestBuffer(bufferQueue, 1);
    }

    private RandomStringSource createObjectUnderTest() {
        return new RandomStringSource();
    }

    @Test
    void testPutRecord() {
        final RandomStringSource randomStringSource = createObjectUnderTest();

        randomStringSource.start(buffer);
        await().atMost(3, TimeUnit.SECONDS)
                .pollDelay(200, TimeUnit.MILLISECONDS)
                .until(() -> buffer.size() > 0);
        assertThat(buffer.size(), greaterThan(0));
    }

    @Test
    void testStop() throws InterruptedException {
        final RandomStringSource randomStringSource = createObjectUnderTest();
        //Start source, and sleep for 1000 millis
        randomStringSource.start(buffer);
        await().atMost(3, TimeUnit.SECONDS)
                .pollDelay(200, TimeUnit.MILLISECONDS)
                .until(() -> buffer.size() > 0);
        //Stop the source, and wait long enough that another message would be sent
        //if the source was running
        randomStringSource.stop();
        Thread.sleep(200);  // Ensure the other thread has time to finish writing.
        final int sizeAfterCompletion = buffer.size();
        Thread.sleep(1000);
        assertThat(buffer.size(), equalTo(sizeAfterCompletion));
    }

}
