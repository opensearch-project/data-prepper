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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;

import java.io.IOException;
import java.time.Duration;

@ExtendWith(MockitoExtension.class)
class ThresholdCheckTest {

    @Mock(lenient = true)
    private Buffer buffer;
    @Mock
    Record record;
    @Mock
    Event event;
    private int maxEvents;
    private ByteCount maxBytes;
    private Duration maxCollectionDuration;
    private Boolean isBatchEnabled;

    @BeforeEach
    void setUp() {
        maxEvents = 10_000;
        maxBytes = ByteCount.parse("1mb");
        maxCollectionDuration = Duration.ofMinutes(5);
        isBatchEnabled = true;
        when(record.getData()).thenReturn(event);
        when(event.toJsonString()).thenReturn("{}");
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxEvents_is_greater_than_buffered_event_count() throws IOException {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents + 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration, record);

        assertTrue(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxEvents_is_less_than_buffered_event_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(this.maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);

        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxBytes_is_greater_than_buffered_byte_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() + 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);

        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxBytes_is_less_than_buffered_byte_count() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);

        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_true_dueTo_maxCollectionDuration_is_greater_than_buffered_event_collection_duration() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.plusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);

        assertTrue(isThresholdExceed, "Threshold not exceeded");
    }

    @Test
    void test_exceedThreshold_false_dueTo_maxCollectionDuration_is_less_than_buffered_event_collection_duration() {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);


        assertFalse(isThresholdExceed, "Threshold exceeded");
    }

    @Test
    void test_exceedThreshold_when_batch_is_enabled() throws IOException {
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1000);
        when(buffer.getEventCount()).thenReturn(maxEvents + 1);
        when(buffer.getDuration()).thenReturn(maxCollectionDuration.minusSeconds(1));
        Boolean isBatchEnabled = false;

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration,record);

        assertTrue(isThresholdExceed);
    }

    @Test
    void test_exceedThreshold_false_when_payload_is_within_limit() {
        // Set up the buffer with a small size and low event count,
        // ensuring that adding the record (which has an estimated size of 2 bytes "{}")
        // does not exceed any of the thresholds.
        when(buffer.getSize()).thenReturn(100L);
        when(buffer.getEventCount()).thenReturn(5);
        when(buffer.getDuration()).thenReturn(Duration.ofMinutes(2));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration, record);

        assertFalse(isThresholdExceed, "Threshold should not be exceeded when payload is within limit");
    }

    @Test
    void test_exceedThreshold_true_when_payload_will_exceed_due_to_record_size() {
        // Assume record estimated size is 2 bytes (from "{}").
        // Set buffer size to maxBytes-1, so adding the record (2 bytes) exceeds the limit.
        when(buffer.getSize()).thenReturn(maxBytes.getBytes() - 1);
        when(buffer.getEventCount()).thenReturn(5);
        when(buffer.getDuration()).thenReturn(Duration.ofMinutes(2));

        boolean isThresholdExceed = ThresholdCheck.checkThresholdExceed(buffer, maxEvents,
                maxBytes, maxCollectionDuration, record);

        assertTrue(isThresholdExceed, "Threshold should be exceeded due to record size pushing payload over the limit");
    }
}
