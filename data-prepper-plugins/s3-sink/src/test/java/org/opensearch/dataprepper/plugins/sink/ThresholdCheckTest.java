/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThresholdCheckTest {

    private Buffer inMemoryBuffer;

    @BeforeEach
    void setUp() throws IOException {
        inMemoryBuffer = new InMemoryBufferFactory().getBuffer();

        while (inMemoryBuffer.getEventCount() < 100) {
            inMemoryBuffer.writeEvent(generateByteArray());
        }
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxEvents_is_less_than_buffered_event_count() {
        final int maxEvents = 95;
        final ByteCount maxBytes = ByteCount.parse("50kb");
        final long maxCollectionDuration = 15;
        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer, maxEvents,
                maxBytes, maxCollectionDuration);
        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxEvents_is_greater_than_buffered_event_count() {
        final int maxEvents = 105;
        final ByteCount maxBytes = ByteCount.parse("50mb");
        final long maxCollectionDuration = 50;
        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer, maxEvents, maxBytes,
                maxCollectionDuration);
        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_ture_dueTo_maxBytes_is_less_than_buffered_byte_count() {
        final int maxEvents = 500;
        final ByteCount maxBytes = ByteCount.parse("1b");
        final long maxCollectionDuration = 15;
        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer, maxEvents, maxBytes,
                maxCollectionDuration);
        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxBytes_is_greater_than_buffered_byte_count() {
        final int maxEvents = 500;
        final ByteCount maxBytes = ByteCount.parse("8mb");
        final long maxCollectionDuration = 15;
        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer, maxEvents,
                maxBytes, maxCollectionDuration);
        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_ture_dueTo_maxCollectionDuration_is_less_than_buffered_event_collection_duration()
            throws IOException, InterruptedException {
        final int maxEvents = 500;
        final ByteCount maxBytes = ByteCount.parse("500mb");
        final long maxCollectionDuration = 10;

        inMemoryBuffer = new InMemoryBufferFactory().getBuffer();
        boolean isThresholdExceed = Boolean.FALSE;
        synchronized (this) {
            while (inMemoryBuffer.getEventCount() < 100) {
                inMemoryBuffer.writeEvent(generateByteArray());
                isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer, maxEvents,
                        maxBytes, maxCollectionDuration);
                if (isThresholdExceed) {
                    break;
                }
                wait(5000);
            }
        }
        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_ture_dueTo_maxCollectionDuration_is_greater_than_buffered_event_collection_duration()
            throws IOException, InterruptedException {
        final int maxEvents = 500;
        final ByteCount maxBytes = ByteCount.parse("500mb");
        final long maxCollectionDuration = 240;

        inMemoryBuffer = new InMemoryBufferFactory().getBuffer();

        boolean isThresholdExceed = Boolean.FALSE;
        synchronized (this) {
            while (inMemoryBuffer.getEventCount() < 100) {
                inMemoryBuffer.writeEvent(generateByteArray());
                isThresholdExceed = ThresholdCheck.checkThresholdExceed(inMemoryBuffer,
                        maxEvents, maxBytes, maxCollectionDuration);
                if (isThresholdExceed) {
                    break;
                }
                wait(50);
            }
        }
        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[10000];
        for (int i = 0; i < 10000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
