/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.confluence.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContentHistoryTest {

    @Test
    void testGetCreatedDateInMillis_ValidDate() {
        ContentHistory history = new ContentHistory();
        history.setCreatedDate("2025-02-17T23:34:44.633Z");

        long expectedMillis = 1739835284633L; // Pre-calculated value for this timestamp
        assertEquals(expectedMillis, history.getCreatedDateInMillis());
    }

    @Test
    void testGetLastModifiedInMillis_ValidDate() {
        ContentHistory history = new ContentHistory();
        ContentHistory.LastUpdated lastUpdated = new ContentHistory.LastUpdated();
        lastUpdated.setWhen("2025-02-17T23:34:44.633Z");
        history.setLastUpdated(lastUpdated);
        long expectedMillis = 1739835284633L; // Pre-calculated value for this timestamp
        assertEquals(expectedMillis, history.getLastUpdatedInMillis());
    }

    @Test
    void testGetCreatedDateInMillis_NullDate() {
        ContentHistory history = new ContentHistory();
        history.setCreatedDate(null);
        assertEquals(0L, history.getCreatedDateInMillis());
    }

    @Test
    void testGetCreatedDateInMillis_InvalidDate() {
        ContentHistory history = new ContentHistory();
        history.setCreatedDate("invalid-date");
        assertEquals(0L, history.getCreatedDateInMillis());
    }
}

