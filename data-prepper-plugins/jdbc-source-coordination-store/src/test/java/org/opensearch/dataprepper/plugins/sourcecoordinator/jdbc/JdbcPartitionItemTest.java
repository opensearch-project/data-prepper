/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JdbcPartitionItemTest {

    @Test
    void getters_and_setters() {
        final JdbcPartitionItem item = new JdbcPartitionItem();
        final Instant now = Instant.now();

        item.setSourceIdentifier("source-1");
        item.setSourcePartitionKey("partition-1");
        item.setPartitionOwner("owner-1");
        item.setPartitionProgressState("{\"offset\":100}");
        item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item.setPartitionOwnershipTimeout(now);
        item.setReOpenAt(now.plusSeconds(60));
        item.setClosedCount(3L);
        item.setPartitionPriority(now.toString());
        item.setVersion(5);

        assertEquals("source-1", item.getSourceIdentifier());
        assertEquals("partition-1", item.getSourcePartitionKey());
        assertEquals("owner-1", item.getPartitionOwner());
        assertEquals("{\"offset\":100}", item.getPartitionProgressState());
        assertEquals(SourcePartitionStatus.ASSIGNED, item.getSourcePartitionStatus());
        assertEquals(now, item.getPartitionOwnershipTimeout());
        assertEquals(now.plusSeconds(60), item.getReOpenAt());
        assertEquals(3L, item.getClosedCount());
        assertEquals(now.toString(), item.getPartitionPriority());
        assertEquals(5, item.getVersion());
    }

    @Test
    void defaults_are_null_or_zero() {
        final JdbcPartitionItem item = new JdbcPartitionItem();

        assertNull(item.getSourceIdentifier());
        assertNull(item.getSourcePartitionKey());
        assertNull(item.getPartitionOwner());
        assertNull(item.getPartitionProgressState());
        assertNull(item.getSourcePartitionStatus());
        assertNull(item.getPartitionOwnershipTimeout());
        assertNull(item.getReOpenAt());
        assertNull(item.getClosedCount());
        assertNull(item.getPartitionPriority());
        assertEquals(0, item.getVersion());
    }
}
