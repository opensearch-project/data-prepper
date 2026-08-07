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
 * A timestamped annotation of associated attributes for a span.
 * @since 1.2
 */
public interface SpanEvent {

    /**
     * Gets the name of the event
     * @return the name
     * @since 1.2
     */
    String getName();

    /**
     * Gets the time the event occurred.
     * @return the time
     * @since 1.2
     */
    String getTime();

    /**
     * Gets a map of user-supplied attributes.
     * @return a map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the number of dropped attributes. 0 indicates no attributes were dropped.
     * @return the number of dropped attributes
     * @since 1.2
     */
    Integer getDroppedAttributesCount();
}
