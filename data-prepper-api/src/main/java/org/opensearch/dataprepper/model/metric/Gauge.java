/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

/**
 * A gauge event in Data Prepper represents a metric event.
 * @since 1.4
 */
public interface Gauge extends Metric {

    /**
     * Gets the value for a gauge
     * @return the value
     * @since 1.4
     */
    Double getValue();
}
