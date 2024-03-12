/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class RandomStringSourceTests {
    @Mock
    private Buffer<Record<Event>> buffer;

    private RandomStringSource createObjectUnderTest() {
        return new RandomStringSource(new RandomStringSourceConfig());
    }

    @Test
    void testPutRecord() {
        final RandomStringSource randomStringSource = createObjectUnderTest();

        randomStringSource.start(buffer);
        await().atMost(3, TimeUnit.SECONDS)
                .pollDelay(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(buffer).write(any(), anyInt()));

        randomStringSource.stop();
    }

    @Test
    void source_continues_to_write_if_a_write_to_buffer_fails() throws TimeoutException {
        final RandomStringSource randomStringSource = createObjectUnderTest();

        doThrow(TimeoutException.class).when(buffer).write(any(), anyInt());

        randomStringSource.start(buffer);
        await().atMost(3, TimeUnit.SECONDS)
                .pollDelay(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(buffer, atLeast(2)).write(any(), anyInt()));

        randomStringSource.stop();
    }

    @Test
    void testStop() throws InterruptedException, TimeoutException {
        final RandomStringSource randomStringSource = createObjectUnderTest();
        //Start source, and sleep for 1000 millis
        randomStringSource.start(buffer);
        await().atMost(3, TimeUnit.SECONDS)
                .pollDelay(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(buffer).write(any(), anyInt()));
        //Stop the source, and wait long enough that another message would be sent
        //if the source was running
        randomStringSource.stop();
        Thread.sleep(200);  // Ensure the other thread has time to finish writing.
        verify(buffer, atLeastOnce()).write(any(), anyInt());
        Thread.sleep(1000);
        verifyNoMoreInteractions(buffer);
    }

    @Test
    void multiple_calls_to_start_throws() {
        final RandomStringSource objectUnderTest = createObjectUnderTest();

        objectUnderTest.start(buffer);

        assertThrows(IllegalStateException.class, () -> objectUnderTest.start(buffer));

        objectUnderTest.stop();
    }
}
