/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;

/**
 * Check threshold limits.
 */
public class ThresholdCheck {

    private ThresholdCheck() {
    }

    /**
     * Check threshold exceeds.
     * @param currentBuffer current buffer.
     * @param maxEvents maximum event provided by user as threshold.
     * @param maxBytes maximum bytes provided by user as threshold.
     * @param maxCollectionDuration maximum event collection duration provided by user as threshold.
     * @return boolean value whether the threshold are met.
     */
    public static boolean checkThresholdExceed(final Buffer currentBuffer, final int maxEvents, final ByteCount maxBytes, final long maxCollectionDuration) {
        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }
}
