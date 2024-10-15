/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class EMFMetricUtilsTest {
    @Test
    void testClampZero() {
        final double testValue = -0;
        assertThat(EMFMetricUtils.clampMetricValue(testValue), equalTo(Math.abs(testValue)));
    }

    @Test
    void testClampMagnitudeWithinRange() {
        final double testPositiveValue = 10.1;
        final double testNegativeValue = -10.1;
        assertThat(EMFMetricUtils.clampMetricValue(testPositiveValue), equalTo(testPositiveValue));
        assertThat(EMFMetricUtils.clampMetricValue(testNegativeValue), equalTo(testNegativeValue));
        assertThat(EMFMetricUtils.clampMetricValue(EMFMetricUtils.MAXIMUM_ALLOWED_VALUE - 1), equalTo(EMFMetricUtils.MAXIMUM_ALLOWED_VALUE - 1));
        assertThat(EMFMetricUtils.clampMetricValue(-EMFMetricUtils.MAXIMUM_ALLOWED_VALUE + 1), equalTo(-EMFMetricUtils.MAXIMUM_ALLOWED_VALUE + 1));
    }

    @Test
    void testClampMagnitudeOverFlow() {
        assertThat(EMFMetricUtils.clampMetricValue(Double.POSITIVE_INFINITY), equalTo(EMFMetricUtils.MAXIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(Double.NEGATIVE_INFINITY), equalTo(-EMFMetricUtils.MAXIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(EMFMetricUtils.MAXIMUM_ALLOWED_VALUE + 1), equalTo(EMFMetricUtils.MAXIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(-EMFMetricUtils.MAXIMUM_ALLOWED_VALUE - 1), equalTo(-EMFMetricUtils.MAXIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(EMFMetricUtils.MINIMUM_ALLOWED_VALUE / 2), equalTo(EMFMetricUtils.MINIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(-EMFMetricUtils.MINIMUM_ALLOWED_VALUE / 2), equalTo(-EMFMetricUtils.MINIMUM_ALLOWED_VALUE));
    }

    @Test
    void testClampMagnitudeTooSmall() {
        assertThat(EMFMetricUtils.clampMetricValue(Double.MIN_VALUE), equalTo(EMFMetricUtils.MINIMUM_ALLOWED_VALUE));
        assertThat(EMFMetricUtils.clampMetricValue(-Double.MIN_VALUE), equalTo(-EMFMetricUtils.MINIMUM_ALLOWED_VALUE));
    }

    @Test
    void testClampNaN() {
        assertThat(EMFMetricUtils.clampMetricValue(Double.NaN), equalTo(Double.NaN));
    }
}