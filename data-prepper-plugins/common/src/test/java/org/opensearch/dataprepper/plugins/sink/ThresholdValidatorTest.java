/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;

class ThresholdValidatorTest {

    private Buffer inMemoryBuffer;

    @BeforeEach
    void setUp() throws IOException {
        inMemoryBuffer = new InMemoryBufferFactory().getBuffer();
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxEvents_is_greater_than_buffered_event_count() {
        final int maxEvents = 105;
        final ByteCount maxBytes = ByteCount.parse("50mb");
        final long maxCollectionDuration = 50;
        boolean isThresholdExceed = ThresholdValidator.checkThresholdExceed(inMemoryBuffer, maxEvents, maxBytes,
                maxCollectionDuration);
        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxBytes_is_greater_than_buffered_byte_count() {
        final int maxEvents = 500;
        final ByteCount maxBytes = ByteCount.parse("8mb");
        final long maxCollectionDuration = 15;
        boolean isThresholdExceed = ThresholdValidator.checkThresholdExceed(inMemoryBuffer, maxEvents,
                maxBytes, maxCollectionDuration);
        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

}
