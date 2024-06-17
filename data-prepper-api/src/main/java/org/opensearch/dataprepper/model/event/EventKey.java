/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

/**
 * Model class to represent a key into a Data Prepper {@link Event}.
 *
 * @since 2.9
 */
public interface EventKey {
    /**
     * The original key provided as a string.
     *
     * @return The key as a string
     * @since 2.9
     */
    String getKey();
}
