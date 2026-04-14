/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcSourceCoordinationStoreIT {

    private static final String JDBC_URL = getProperty("tests.jdbc.url", "jdbc:postgresql://localhost:5433/dataprepper");
    private static final String JDBC_USER = getProperty("tests.jdbc.username", "dp_user");
    private static final String JDBC_PASS = getProperty("tests.jdbc.password", "dp_pass");

    private static String getProperty(final String key, final String defaultValue) {
        final String value = System.getProperty(key);
        return value != null && !value.isEmpty() ? value : defaultValue;
    }

    private JdbcSourceCoordinationStore store;
    private static final String SOURCE_ID = "test-source";

    @BeforeEach
    void setUp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS source_coordination");
        }

        final JdbcStoreSettings settings = new JdbcStoreSettings(
                JDBC_URL, JDBC_USER, JDBC_PASS,
                null, null, null, null, null);
        store = new JdbcSourceCoordinationStore(settings);
        store.initializeStore();
    }

    @Test
    void initializeStore_creates_table() {
        assertTrue(store.tryCreatePartitionItem(
                SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false));
    }

    @Test
    void initializeStore_is_idempotent() {
        store.initializeStore();
        assertTrue(store.tryCreatePartitionItem(
                SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false));
    }

    @Test
    void tryCreatePartitionItem_returns_false_for_duplicate() {
        assertTrue(store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false));
        assertFalse(store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false));
    }

    @Test
    void getSourcePartitionItem_returns_created_item() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, "{\"k\":\"v\"}", false);

        final Optional<SourcePartitionStoreItem> result = store.getSourcePartitionItem(SOURCE_ID, "p1");
        assertTrue(result.isPresent());
        assertEquals(SOURCE_ID, result.get().getSourceIdentifier());
        assertEquals("p1", result.get().getSourcePartitionKey());
        assertEquals(SourcePartitionStatus.UNASSIGNED, result.get().getSourcePartitionStatus());
        assertEquals("{\"k\":\"v\"}", result.get().getPartitionProgressState());
        assertEquals(0L, result.get().getClosedCount());
    }

    @Test
    void getSourcePartitionItem_returns_empty_for_nonexistent() {
        assertFalse(store.getSourcePartitionItem(SOURCE_ID, "nonexistent").isPresent());
    }

    @Test
    void tryUpdateSourcePartitionItem_updates_with_version_check() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        final SourcePartitionStoreItem item = store.getSourcePartitionItem(SOURCE_ID, "p1").get();

        item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item.setPartitionOwner("node-1");
        item.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));
        store.tryUpdateSourcePartitionItem(item);

        final SourcePartitionStoreItem updated = store.getSourcePartitionItem(SOURCE_ID, "p1").get();
        assertEquals(SourcePartitionStatus.ASSIGNED, updated.getSourcePartitionStatus());
        assertEquals("node-1", updated.getPartitionOwner());
        assertNotNull(updated.getPartitionOwnershipTimeout());
    }

    @Test
    void tryUpdateSourcePartitionItem_throws_on_version_mismatch() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        final SourcePartitionStoreItem item1 = store.getSourcePartitionItem(SOURCE_ID, "p1").get();
        final SourcePartitionStoreItem item2 = store.getSourcePartitionItem(SOURCE_ID, "p1").get();

        item1.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item1.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));
        store.tryUpdateSourcePartitionItem(item1);

        item2.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item2.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));
        assertThrows(PartitionUpdateException.class, () -> store.tryUpdateSourcePartitionItem(item2));
    }

    @Test
    void tryAcquireAvailablePartition_acquires_unassigned() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        store.tryCreatePartitionItem(SOURCE_ID, "p2", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        final Optional<SourcePartitionStoreItem> acquired = store.tryAcquireAvailablePartition(
                SOURCE_ID, "node-1", Duration.ofMinutes(10));

        assertTrue(acquired.isPresent());
        assertEquals(SourcePartitionStatus.ASSIGNED, acquired.get().getSourcePartitionStatus());
        assertEquals("node-1", acquired.get().getPartitionOwner());
    }

    @Test
    void tryAcquireAvailablePartition_returns_empty_when_none_available() {
        assertFalse(store.tryAcquireAvailablePartition(
                SOURCE_ID, "node-1", Duration.ofMinutes(10)).isPresent());
    }

    @Test
    void tryAcquireAvailablePartition_acquires_expired_assigned() throws InterruptedException {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        final Optional<SourcePartitionStoreItem> first = store.tryAcquireAvailablePartition(
                SOURCE_ID, "node-1", Duration.ofSeconds(2));
        assertTrue(first.isPresent());

        Thread.sleep(3000);

        final Optional<SourcePartitionStoreItem> second = store.tryAcquireAvailablePartition(
                SOURCE_ID, "node-2", Duration.ofMinutes(10));
        assertTrue(second.isPresent());
        assertEquals("node-2", second.get().getPartitionOwner());
    }

    @Test
    void tryAcquireAvailablePartition_acquires_closed_with_expired_reopen() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        final SourcePartitionStoreItem item = store.getSourcePartitionItem(SOURCE_ID, "p1").get();

        item.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
        item.setReOpenAt(Instant.now().minusSeconds(10));
        item.setClosedCount(1L);
        store.tryUpdateSourcePartitionItem(item);

        final Optional<SourcePartitionStoreItem> acquired = store.tryAcquireAvailablePartition(
                SOURCE_ID, "node-1", Duration.ofMinutes(10));
        assertTrue(acquired.isPresent());
        assertEquals("node-1", acquired.get().getPartitionOwner());
    }

    @Test
    void tryDeletePartitionItem_deletes_item() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        final SourcePartitionStoreItem item = store.getSourcePartitionItem(SOURCE_ID, "p1").get();

        store.tryDeletePartitionItem(item);

        assertFalse(store.getSourcePartitionItem(SOURCE_ID, "p1").isPresent());
    }

    @Test
    void queryAllSourcePartitionItems_returns_all_for_source() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        store.tryCreatePartitionItem(SOURCE_ID, "p2", SourcePartitionStatus.ASSIGNED, 0L, null, false);
        store.tryCreatePartitionItem("other-source", "p3", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        final List<SourcePartitionStoreItem> items = store.queryAllSourcePartitionItems(SOURCE_ID);
        assertEquals(2, items.size());
    }

    @Test
    void querySourcePartitionItemsByStatus_filters_correctly() {
        store.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        store.tryCreatePartitionItem(SOURCE_ID, "p2", SourcePartitionStatus.ASSIGNED, 0L, null, false);

        final List<SourcePartitionStoreItem> items = store.querySourcePartitionItemsByStatus(
                SOURCE_ID, SourcePartitionStatus.UNASSIGNED, Instant.EPOCH.toString());
        assertEquals(1, items.size());
        assertEquals("p1", items.get(0).getSourcePartitionKey());
    }

    @Test
    void deleteExpiredItems_deletes_completed_partitions_past_ttl() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS source_coordination");
        }

        final JdbcStoreSettings ttlSettings = new JdbcStoreSettings(
                JDBC_URL, JDBC_USER, JDBC_PASS,
                null, null, null, Duration.ofSeconds(2), null);
        final JdbcSourceCoordinationStore ttlStore = new JdbcSourceCoordinationStore(ttlSettings);
        ttlStore.initializeStore();

        ttlStore.tryCreatePartitionItem(SOURCE_ID, "p1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);
        final SourcePartitionStoreItem item = ttlStore.getSourcePartitionItem(SOURCE_ID, "p1").get();
        item.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        ttlStore.tryUpdateSourcePartitionItem(item);

        assertTrue(ttlStore.getSourcePartitionItem(SOURCE_ID, "p1").isPresent());

        Thread.sleep(3000);

        ttlStore.deleteExpiredItems();

        assertFalse(ttlStore.getSourcePartitionItem(SOURCE_ID, "p1").isPresent());
    }
}
