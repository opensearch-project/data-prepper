/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ThreadSleeperTest {
    private ThreadSleeper target;

    @BeforeEach
    void setUp() {
        target = new ThreadSleeper();
    }

    @Test
    void testSleepDoesNotThrowWhenNotInterrupted() {
        try {
            target.sleep(1);
        } catch (InterruptedException e) {
            fail("Sleep was interrupted unexpectedly");
        }
    }

    @Test
    void testSleepThrowsInterruptedExceptionIfThreadInterrupted() {
        Thread thread = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                target.sleep(10);
                fail("Expected InterruptedException");
            } catch (InterruptedException e) {
                assertTrue(Thread.currentThread().isInterrupted(), "Thread should remain interrupted");
            }
        });

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            fail("Test thread was interrupted");
        }
    }
}
