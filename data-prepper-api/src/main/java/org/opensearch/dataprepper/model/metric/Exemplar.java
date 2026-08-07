/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

import java.util.Map;

/**
 * A representation of an exemplar which is a sample input measurement.
 * It may contain the span and trace id of a metric event.
 *
 * @since 1.4
 */
public interface Exemplar {

    /**
     * Gets the string encoded value of time_unix_nano
     * @return the time value
     * @since 1.4
     */
    String getTime();


    /**
     * Gets the value for the exemplar
     * @return the value
     * @since 1.4
     */
    Double getValue();

    /**
     * Gets a collection of key-value pairs related to the exemplar.
     *
     * @return A map of attributes
     * @since 1.4
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the span id of this exemplar.
     *
     * @return the span id
     * @since 1.4
     */
    String getSpanId();

    /**
     * Gets the trace id of this exemplar.
     *
     * @return the trace id
     * @since 1.4
     */
    String getTraceId();

}
