/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JdbcSourceCoordinationStoreTest {

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @Mock
    private HikariDataSource mockDataSource;

    private JdbcStoreSettings settings;
    private JdbcSourceCoordinationStore store;

    @BeforeEach
    void setUp() throws SQLException {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, null, null);
        lenient().when(mockDataSource.getConnection()).thenReturn(connection);
        lenient().when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    private void createInitializedStore() {
        store = new JdbcSourceCoordinationStore(settings) {
            @Override
            HikariDataSource createDataSource(final HikariConfig hikariConfig) {
                return mockDataSource;
            }
        };
        store.initializeStore();
    }

    @Test
    void initializeStore_second_call_is_skipped() {
        final int[] createDataSourceCallCount = {0};
        store = new JdbcSourceCoordinationStore(settings) {
            @Override
            HikariDataSource createDataSource(final HikariConfig hikariConfig) {
                createDataSourceCallCount[0]++;
                return mockDataSource;
            }
        };
        store.initializeStore();
        store.initializeStore();

        assertThat(createDataSourceCallCount[0], equalTo(1));
    }

    @Test
    void getSourcePartitionItem_returns_item_when_found() throws Exception {
        createInitializedStore();

        when(resultSet.next()).thenReturn(true, false);
        mockResultSetRow();
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        final Optional<SourcePartitionStoreItem> result = store.getSourcePartitionItem("source-1", "partition-1");

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getSourceIdentifier(), equalTo("source-1"));
    }

    @Test
    void getSourcePartitionItem_returns_empty_when_not_found() throws Exception {
        createInitializedStore();

        when(resultSet.next()).thenReturn(false);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        final Optional<SourcePartitionStoreItem> result = store.getSourcePartitionItem("source-1", "nonexistent");

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void getSourcePartitionItem_returns_empty_on_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeQuery()).thenThrow(new SQLException("connection error"));

        final Optional<SourcePartitionStoreItem> result = store.getSourcePartitionItem("source-1", "partition-1");

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void querySourcePartitionItemsByStatus_returns_matching_items() throws Exception {
        createInitializedStore();

        when(resultSet.next()).thenReturn(true, false);
        mockResultSetRow();
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        final List<SourcePartitionStoreItem> result = store.querySourcePartitionItemsByStatus(
                "source-1", SourcePartitionStatus.UNASSIGNED, Instant.EPOCH.toString());

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getSourceIdentifier(), equalTo("source-1"));
        assertThat(result.get(0).getSourcePartitionStatus(), equalTo(SourcePartitionStatus.UNASSIGNED));
    }

    @Test
    void querySourcePartitionItemsByStatus_returns_empty_on_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeQuery()).thenThrow(new SQLException("error"));

        final List<SourcePartitionStoreItem> result = store.querySourcePartitionItemsByStatus(
                "source-1", SourcePartitionStatus.UNASSIGNED, Instant.EPOCH.toString());

        assertThat(result, is(empty()));
    }

    @Test
    void queryAllSourcePartitionItems_returns_items() throws Exception {
        createInitializedStore();

        when(resultSet.next()).thenReturn(true, true, false);
        mockResultSetRow();
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        final List<SourcePartitionStoreItem> result = store.queryAllSourcePartitionItems("source-1");

        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getSourceIdentifier(), equalTo("source-1"));
    }

    @Test
    void queryAllSourcePartitionItems_returns_empty_on_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeQuery()).thenThrow(new SQLException("error"));

        final List<SourcePartitionStoreItem> result = store.queryAllSourcePartitionItems("source-1");

        assertThat(result, is(empty()));
    }

    @Test
    void tryCreatePartitionItem_returns_true_on_success() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        assertThat(result, is(true));
    }

    @Test
    void tryCreatePartitionItem_returns_false_on_constraint_violation_sqlstate() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("duplicate", "23505"));

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        assertThat(result, is(false));
    }

    @Test
    void tryCreatePartitionItem_returns_false_on_integrity_constraint_violation() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLIntegrityConstraintViolationException("duplicate"));

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        assertThat(result, is(false));
    }

    @Test
    void tryCreatePartitionItem_with_ttl_sets_expiration_time() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, Duration.ofHours(1), null);
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        assertThat(result, is(true));
        verify(preparedStatement, never()).setNull(7, Types.TIMESTAMP);
    }

    @Test
    void tryCreatePartitionItem_readonly_does_not_set_expiration_time() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, Duration.ofHours(1), null);
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, true);

        assertThat(result, is(true));
        verify(preparedStatement).setNull(7, Types.TIMESTAMP);
    }

    @Test
    void tryUpdateSourcePartitionItem_succeeds_when_version_matches() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.ASSIGNED, 0);
        item.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));

        store.tryUpdateSourcePartitionItem(item);

        assertThat(item.getVersion(), equalTo(1L));
    }

    @Test
    void tryUpdateSourcePartitionItem_throws_on_version_mismatch() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(0);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.ASSIGNED, 0);

        assertThrows(PartitionUpdateException.class, () -> store.tryUpdateSourcePartitionItem(item));
    }

    @Test
    void tryUpdateSourcePartitionItem_throws_on_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("error"));

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.ASSIGNED, 0);

        assertThrows(PartitionUpdateException.class, () -> store.tryUpdateSourcePartitionItem(item));
    }

    @Test
    void tryUpdateSourcePartitionItem_sets_priority_for_closed_status() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final Instant reopenAt = Instant.now().plusSeconds(300);
        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.CLOSED, 0);
        item.setReOpenAt(reopenAt);

        store.tryUpdateSourcePartitionItem(item);

        assertThat(item.getPartitionPriority(), equalTo(reopenAt.toString()));
    }

    @Test
    void tryUpdateSourcePartitionItem_with_priority_override_for_unassigned() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final Instant priorityOverride = Instant.now();
        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0);

        store.tryUpdateSourcePartitionItem(item, priorityOverride);

        assertThat(item.getPartitionPriority(), equalTo(priorityOverride.toString()));
    }

    @Test
    void tryDeletePartitionItem_succeeds_when_version_matches() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0);

        store.tryDeletePartitionItem(item);

        verify(preparedStatement).executeUpdate();
    }

    @Test
    void tryDeletePartitionItem_throws_on_version_mismatch() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(0);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0);

        assertThrows(PartitionUpdateException.class, () -> store.tryDeletePartitionItem(item));
    }

    @Test
    void tryDeletePartitionItem_throws_on_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("error"));

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0);

        assertThrows(PartitionUpdateException.class, () -> store.tryDeletePartitionItem(item));
    }

    @Test
    void tryAcquireAvailablePartition_returns_empty_when_no_rows() throws Exception {
        createInitializedStore();

        when(resultSet.next()).thenReturn(false);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        final Optional<SourcePartitionStoreItem> result = store.tryAcquireAvailablePartition(
                "source-1", "node-1", Duration.ofMinutes(10));

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryAcquireAvailablePartition_acquires_available_row() throws Exception {
        createInitializedStore();

        final ResultSet emptyRs = mock(ResultSet.class);
        when(emptyRs.next()).thenReturn(false);

        final ResultSet unassignedRs = mock(ResultSet.class);
        when(unassignedRs.next()).thenReturn(true, false);
        mockResultSetOnRs(unassignedRs);

        final PreparedStatement stmt1 = mock(PreparedStatement.class);
        when(stmt1.executeQuery()).thenReturn(emptyRs);

        final PreparedStatement stmt2 = mock(PreparedStatement.class);
        when(stmt2.executeQuery()).thenReturn(unassignedRs);

        final PreparedStatement updateStmt = mock(PreparedStatement.class);
        when(updateStmt.executeUpdate()).thenReturn(1);

        when(connection.prepareStatement(anyString()))
                .thenReturn(stmt1)
                .thenReturn(stmt2)
                .thenReturn(updateStmt);

        final Optional<SourcePartitionStoreItem> result = store.tryAcquireAvailablePartition(
                "source-1", "node-1", Duration.ofMinutes(10));

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getPartitionOwner(), equalTo("node-1"));
        assertThat(result.get().getSourcePartitionStatus(), equalTo(SourcePartitionStatus.ASSIGNED));
    }

    @Test
    void tryAcquireAvailablePartition_returns_empty_on_sql_exception() throws Exception {
        createInitializedStore();

        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("error"));

        final Optional<SourcePartitionStoreItem> result = store.tryAcquireAvailablePartition(
                "source-1", "node-1", Duration.ofMinutes(10));

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void deleteExpiredItems_executes_delete() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(3);

        store.deleteExpiredItems();

        verify(preparedStatement).executeUpdate();
    }

    @Test
    void deleteExpiredItems_handles_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("error"));

        store.deleteExpiredItems();

        verify(preparedStatement).executeUpdate();
    }

    @Test
    void close_shuts_down_data_source() {
        createInitializedStore();

        store.close();

        verify(mockDataSource).close();
    }

    @Test
    void close_shuts_down_ttl_executor() {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, Duration.ofHours(1), null);
        createInitializedStore();

        store.close();

        verify(mockDataSource).close();
        // ttlExecutor is created internally when TTL is configured.
        // We verify indirectly that close() does not throw when ttlExecutor is non-null.
    }

    @Test
    void close_handles_null_fields() {
        store = new JdbcSourceCoordinationStore(settings);
        store.close();
    }

    @Test
    void initializeStore_creates_table_when_skip_is_false() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, false, null, null, null);
        createInitializedStore();

        // createTable calls prepareStatement for CREATE TABLE and CREATE INDEX
        verify(connection, atLeast(2)).prepareStatement(anyString());
    }

    @Test
    void initializeStore_creates_table_handles_existing_index() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, false, null, null, null);

        final PreparedStatement createTableStmt = mock(PreparedStatement.class);
        final PreparedStatement createIndexStmt = mock(PreparedStatement.class);
        when(createIndexStmt.execute()).thenThrow(new SQLException("already exists", "42P07"));

        when(connection.prepareStatement(anyString()))
                .thenReturn(createTableStmt)
                .thenReturn(createIndexStmt);

        createInitializedStore();

        assertThat(store, is(notNullValue()));
    }

    @Test
    void initializeStore_with_connection_properties() {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, null, Map.of("ssl", "true"));
        createInitializedStore();

        assertThat(store, is(notNullValue()));
    }

    @Test
    void tryUpdateSourcePartitionItem_with_ttl_sets_expiration() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, true, null, Duration.ofHours(1), null);
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.ASSIGNED, 0);
        item.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));

        store.tryUpdateSourcePartitionItem(item);

        assertThat(item.getVersion(), equalTo(1L));
    }

    @Test
    void tryAcquireAvailablePartition_tries_next_on_version_conflict() throws Exception {
        createInitializedStore();

        final ResultSet emptyRs = mock(ResultSet.class);
        when(emptyRs.next()).thenReturn(false);

        final ResultSet unassignedRs = mock(ResultSet.class);
        when(unassignedRs.next()).thenReturn(true, true, false);
        mockResultSetOnRs(unassignedRs);

        final PreparedStatement stmt1 = mock(PreparedStatement.class);
        when(stmt1.executeQuery()).thenReturn(emptyRs);

        final PreparedStatement stmt2 = mock(PreparedStatement.class);
        when(stmt2.executeQuery()).thenReturn(unassignedRs);

        final PreparedStatement failUpdateStmt = mock(PreparedStatement.class);
        when(failUpdateStmt.executeUpdate()).thenReturn(0);

        final PreparedStatement successUpdateStmt = mock(PreparedStatement.class);
        when(successUpdateStmt.executeUpdate()).thenReturn(1);

        when(connection.prepareStatement(anyString()))
                .thenReturn(stmt1)
                .thenReturn(stmt2)
                .thenReturn(failUpdateStmt)
                .thenReturn(successUpdateStmt);

        final Optional<SourcePartitionStoreItem> result = store.tryAcquireAvailablePartition(
                "source-1", "node-1", Duration.ofMinutes(10));

        assertThat(result.isPresent(), is(true));
    }

    @Test
    void createTable_throws_on_non_index_sql_exception() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, false, null, null, null);

        final PreparedStatement createTableStmt = mock(PreparedStatement.class);
        final PreparedStatement createIndexStmt = mock(PreparedStatement.class);
        when(createIndexStmt.execute()).thenThrow(new SQLException("unexpected error", "42000"));

        when(connection.prepareStatement(anyString()))
                .thenReturn(createTableStmt)
                .thenReturn(createIndexStmt);

        assertThrows(RuntimeException.class, () -> createInitializedStore());
    }

    @Test
    void createTable_throws_on_create_table_failure() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, false, null, null, null);

        when(preparedStatement.execute()).thenThrow(new SQLException("connection lost"));

        assertThrows(RuntimeException.class, () -> createInitializedStore());
    }

    @Test
    void createTable_handles_mysql_duplicate_index() throws Exception {
        settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/test", "user", "pass",
                null, false, null, null, null);

        final PreparedStatement createTableStmt = mock(PreparedStatement.class);
        final PreparedStatement createIndexStmt = mock(PreparedStatement.class);
        final SQLException mysqlDuplicate = new SQLException("Duplicate key name", "HY000", 1061);
        when(createIndexStmt.execute()).thenThrow(mysqlDuplicate);

        when(connection.prepareStatement(anyString()))
                .thenReturn(createTableStmt)
                .thenReturn(createIndexStmt);

        createInitializedStore();
    }

    @Test
    void tryCreatePartitionItem_returns_false_on_non_constraint_sql_exception() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("connection lost", "08001"));

        final boolean result = store.tryCreatePartitionItem(
                "source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0L, null, false);

        assertThat(result, is(false));
    }

    @Test
    void tryAcquireAvailablePartition_returns_assigned_expired() throws Exception {
        createInitializedStore();

        final ResultSet assignedRs = mock(ResultSet.class);
        when(assignedRs.next()).thenReturn(true, false);
        mockResultSetOnRs(assignedRs);
        lenient().when(assignedRs.getString("source_partition_status")).thenReturn("ASSIGNED");

        final PreparedStatement queryStmt = mock(PreparedStatement.class);
        when(queryStmt.executeQuery()).thenReturn(assignedRs);

        final PreparedStatement updateStmt = mock(PreparedStatement.class);
        when(updateStmt.executeUpdate()).thenReturn(1);

        when(connection.prepareStatement(anyString()))
                .thenReturn(queryStmt)
                .thenReturn(updateStmt);

        final Optional<SourcePartitionStoreItem> result = store.tryAcquireAvailablePartition(
                "source-1", "node-1", Duration.ofMinutes(10));

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getSourcePartitionStatus(), equalTo(SourcePartitionStatus.ASSIGNED));
    }

    @Test
    void tryUpdateSourcePartitionItem_sets_null_priority_for_closed_with_null_reopen() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.CLOSED, 0);
        item.setReOpenAt(null);

        store.tryUpdateSourcePartitionItem(item);

        assertThat(item.getPartitionPriority(), is(nullValue()));
    }

    @Test
    void tryUpdateSourcePartitionItem_priority_override_ignored_for_non_unassigned() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.ASSIGNED, 0);
        item.setPartitionOwnershipTimeout(Instant.now().plusSeconds(600));

        store.tryUpdateSourcePartitionItem(item, Instant.now());

        assertThat(item.getPartitionPriority(), equalTo(item.getPartitionOwnershipTimeout().toString()));
    }

    @Test
    void tryUpdateSourcePartitionItem_handles_null_closed_count() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(1);

        final JdbcPartitionItem item = createTestItem("source-1", "partition-1", SourcePartitionStatus.UNASSIGNED, 0);
        item.setClosedCount(null);

        store.tryUpdateSourcePartitionItem(item);

        assertThat(item.getVersion(), equalTo(1L));
    }

    @Test
    void deleteExpiredItems_no_items_deleted() throws Exception {
        createInitializedStore();

        when(preparedStatement.executeUpdate()).thenReturn(0);

        store.deleteExpiredItems();

        verify(preparedStatement).executeUpdate();
    }

    private JdbcPartitionItem createTestItem(final String sourceId, final String partitionKey,
                                              final SourcePartitionStatus status, final long version) {
        final JdbcPartitionItem item = new JdbcPartitionItem();
        item.setSourceIdentifier(sourceId);
        item.setSourcePartitionKey(partitionKey);
        item.setSourcePartitionStatus(status);
        item.setVersion(version);
        item.setClosedCount(0L);
        return item;
    }

    private void mockResultSetRow() throws SQLException {
        mockResultSetOnRs(resultSet);
    }

    private void mockResultSetOnRs(final ResultSet rs) throws SQLException {
        lenient().when(rs.getString("source_identifier")).thenReturn("source-1");
        lenient().when(rs.getString("source_partition_key")).thenReturn("partition-1");
        lenient().when(rs.getString("partition_owner")).thenReturn(null);
        lenient().when(rs.getString("partition_progress_state")).thenReturn(null);
        lenient().when(rs.getString("source_partition_status")).thenReturn("UNASSIGNED");
        lenient().when(rs.getObject("partition_ownership_timeout", LocalDateTime.class)).thenReturn(null);
        lenient().when(rs.getObject("reopen_at", LocalDateTime.class)).thenReturn(null);
        lenient().when(rs.getLong("closed_count")).thenReturn(0L);
        lenient().when(rs.getString("partition_priority")).thenReturn(Instant.now().toString());
        lenient().when(rs.getLong("version")).thenReturn(0L);
    }
}
