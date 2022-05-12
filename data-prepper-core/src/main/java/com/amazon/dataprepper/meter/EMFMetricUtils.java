/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

public class EMFMetricUtils {

    private static final double MINIMUM_ALLOWED_VALUE = 8.515920e-109;
    private static final double MAXIMUM_ALLOWED_VALUE = 1.174271e+108;

    static double clampMetricValue(double value) {
        // Leave as is and let the SDK reject it
        if (Double.isNaN(value)) {
            return value;
        }
        double magnitude = Math.abs(value);
        if (magnitude == 0) {
            // Leave zero as zero
            return 0;
        }
        // Non-zero magnitude, clamp to allowed range
        double clampedMag = Math.min(Math.max(magnitude, MINIMUM_ALLOWED_VALUE), MAXIMUM_ALLOWED_VALUE);
        return Math.copySign(clampedMag, value);
    }
}
