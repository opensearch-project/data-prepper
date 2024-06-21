/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.common.lambda.util;

import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.common.lambda.accumlator.Buffer;

import java.time.Duration;

/**
 * Check threshold limits.
 */
public class ThresholdCheck {

    public static boolean checkThresholdExceed(final Buffer currentBuffer, final int maxEvents, final ByteCount maxBytes, final Duration maxCollectionDuration, final Boolean isBatchEnabled) {
        if (!isBatchEnabled) return true;

        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration().compareTo(maxCollectionDuration) > 0 ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration().compareTo(maxCollectionDuration) > 0 ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }
}
