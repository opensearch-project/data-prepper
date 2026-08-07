/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
