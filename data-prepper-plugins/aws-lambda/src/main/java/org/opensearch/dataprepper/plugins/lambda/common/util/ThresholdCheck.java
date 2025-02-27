/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.util;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Check threshold limits.
 */
public class ThresholdCheck {

    public static boolean checkThresholdExceed(final Buffer currentBuffer, final int maxEvents,
                                               final ByteCount maxBytes, final Duration maxCollectionDuration,
                                               Record<Event> nextRecord) {
        int estimatedRecordSize = estimateRecordSize(nextRecord);

        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration().compareTo(maxCollectionDuration) > 0 ||
                    currentBuffer.getSize() + estimatedRecordSize > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration().compareTo(maxCollectionDuration) > 0 ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }

      public static boolean checkTimeoutExceeded(final Buffer currentBuffer, final Duration maxCollectionDuration) {
        return currentBuffer.getDuration().compareTo(maxCollectionDuration) > 0;
    }

     public static boolean checkSizeThresholdExceed(final Buffer currentBuffer, final ByteCount maxBytes, Record<Event> nextRecord) {
        int estimatedRecordSize = estimateRecordSize(nextRecord);
        return (currentBuffer.getSize() + estimatedRecordSize) > maxBytes.getBytes();
    }

    public static boolean checkEventCountThresholdExceeded(final Buffer currentBuffer, final int maxEvents) {
        return currentBuffer.getEventCount() >= maxEvents;
    }

    private static int estimateRecordSize(Record<Event> record) {
        if (record == null || record.getData() == null) {
            return 0;
        }
        String json = record.getData().toJsonString();
        return json.getBytes(StandardCharsets.UTF_8).length;
    }

}
