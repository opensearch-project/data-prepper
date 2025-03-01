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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;

import java.time.Duration;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ThresholdCheckTest {

    @Mock
    private Buffer buffer;
    @Mock
    private Record<Event> record;
    @Mock
    private Event event;

    private int maxEvents;
    private ByteCount maxBytes;
    private Duration maxCollectionDuration;

    @BeforeEach
    void setUp() {
        maxEvents = 10;
        maxBytes = ByteCount.parse("1mb");
        maxCollectionDuration = Duration.ofMinutes(5);

        // Configure the record mock: its event data will produce a simple JSON.
        when(record.getData()).thenReturn(event);
        // For our tests, we assume an empty JSON object "{}" (2 bytes in UTF-8)
        when(event.toJsonString()).thenReturn("{}");
    }

    @Test
    void testTimeoutExceededTrue() {
        // Simulate a buffer that has been open for 6 minutes (exceeding the 5-minute limit)
        when(buffer.getDuration()).thenReturn(Duration.ofMinutes(6));
        assertTrue(ThresholdCheck.checkTimeoutExceeded(buffer, maxCollectionDuration),
                "Expected timeout threshold to be exceeded.");
    }

    @Test
    void testTimeoutExceededFalse() {
        // Simulate a buffer that has been open for 4 minutes (within the limit)
        when(buffer.getDuration()).thenReturn(Duration.ofMinutes(4));
        assertFalse(ThresholdCheck.checkTimeoutExceeded(buffer, maxCollectionDuration),
                "Expected timeout threshold to NOT be exceeded.");
    }


    @Test
    void testSizeThresholdExceedTrue() {
        long maxBytesValue = maxBytes.getBytes();
        when(buffer.getSize()).thenReturn(maxBytesValue - 1);
        // The record's estimated size is 2 bytes (from "{}").
        // So, adding the record yields: (maxBytes - 1) + 2 = maxBytes + 1 (exceeds limit).
        assertTrue(ThresholdCheck.checkSizeThresholdExceed(buffer, maxBytes, record),
                "Expected size threshold to be exceeded due to record size.");
    }

    @Test
    void testSizeThresholdExceedFalse() {
        when(buffer.getSize()).thenReturn(1000L);
        // Estimated record size remains 2 bytes.
        // Total after adding = 1000 + 2, which is less than 1MB.
        assertFalse(ThresholdCheck.checkSizeThresholdExceed(buffer, maxBytes, record),
                "Expected size threshold to NOT be exceeded.");
    }

    @Test
    void testEventCountThresholdExceededTrue() {
        when(buffer.getEventCount()).thenReturn(maxEvents);
        assertTrue(ThresholdCheck.checkEventCountThresholdExceeded(buffer, maxEvents),
                "Expected event count threshold to be reached.");
    }

    @Test
    void testEventCountThresholdExceededFalse() {
        when(buffer.getEventCount()).thenReturn(maxEvents - 1);
        assertFalse(ThresholdCheck.checkEventCountThresholdExceeded(buffer, maxEvents),
                "Expected event count threshold to NOT be reached.");
    }
}
