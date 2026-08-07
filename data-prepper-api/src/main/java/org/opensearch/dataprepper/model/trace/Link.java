/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.trace;

import java.util.Map;

/**
 * A pointer from the current span to another span in a trace.
 * @since 1.2
 */
public interface Link {

    /**
     * Gets the trace id of the linked span. The linked span may or may not be in the same trace.
     * @return the trace id
     * @since 1.2
     */
    String getTraceId();

    /**
     * Gets the span id of the linked span
     * @return the span id
     * @since 1.2
     */
    String getSpanId();

    /**
     * Gets the associated trace state
     * @return the trace state
     * @since 1.2
     */
    String getTraceState();

    /**
     * Gets the attributes associated with the link
     * @return a map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the number of attributes dropped from the link. 0 indicates no links were dropped.
     * @return the number of dropped attributes
     * @since 1.2
     */
    Integer getDroppedAttributesCount();
}
