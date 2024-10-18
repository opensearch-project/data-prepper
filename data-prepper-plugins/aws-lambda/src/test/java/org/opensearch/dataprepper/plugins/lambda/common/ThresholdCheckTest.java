/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;

import java.io.IOException;
import java.time.Duration;

@ExtendWith(MockitoExtension.class)
class ThresholdCheckTest {

    @Mock(lenient = true)
    private Buffer buffer;
    private int maxEvents;
    private ByteCount maxBytes;
    private Duration maxCollectionDuration;
    private Boolean isBatchEnabled;

    @BeforeEach
    void setUp() {
        maxEvents = 10_000;
        maxBytes = ByteCount.parse("48mb");
        maxCollectionDuration = Duration.ofMinutes(5);
        isBatchEnabled = true;
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxEvents_is_greater_than_buffered_event_count() throws IOException {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents + 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertTrue(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxEvents_is_less_than_buffered_event_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(this.maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxBytes_is_greater_than_buffered_byte_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() + 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxBytes_is_less_than_buffered_byte_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxCollectionDuration_is_greater_than_buffered_event_collection_duration() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.plusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxCollectionDuration_is_less_than_buffered_event_collection_duration() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);


        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_when_batch_is_enabled() throws IOException {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents + 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));
        Boolean isBatchEnabled = false;

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration);

        assertTrue(isThresholdExceed);
    }
}
