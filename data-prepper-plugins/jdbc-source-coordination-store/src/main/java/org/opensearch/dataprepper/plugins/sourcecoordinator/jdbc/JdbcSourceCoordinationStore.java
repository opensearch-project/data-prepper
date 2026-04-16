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
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@DataPrepperPlugin(name = "jdbc",
        pluginType = SourceCoordinationStore.class,
        pluginConfigurationType = JdbcStoreSettings.class)
public class JdbcSourceCoordinationStore implements SourceCoordinationStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceCoordinationStore.class);
    private static final String UNIQUE_VIOLATION_SQLSTATE = "23505";
    private static final String DUPLICATE_RELATION_SQLSTATE = "42P07";
    private static final int MYSQL_DUPLICATE_KEY_NAME_ERROR = 1061;
    private static final Duration TTL_CLEANUP_INTERVAL = Duration.ofHours(1);

    private final JdbcStoreSettings settings;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private HikariDataSource dataSource;
    private ScheduledExecutorService ttlExecutor;

    @DataPrepperPluginConstructor
    public JdbcSourceCoordinationStore(final JdbcStoreSettings settings) {
        this.settings = settings;
    }

    @Override
    public void initializeStore() {
        if (!initialized.compareAndSet(false, true)) {
            LOG.debug("JDBC coordination store already initialized, skipping");
            return;
        }
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(settings.getUrl());
        hikariConfig.setMaximumPoolSize(settings.getMaxPoolSize());

        final Properties props = new Properties();
        props.setProperty("user", settings.getUsername());
        props.setProperty("password", settings.getPassword());
        if (settings.getConnectionProperties() != null) {
            props.putAll(settings.getConnectionProperties());
        }
        hikariConfig.setDataSourceProperties(props);

        this.dataSource = createDataSource(hikariConfig);

        if (!settings.skipTableCreation()) {
            createTable();
        }

        if (settings.getTtl() != null) {
            ttlExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread t = new Thread(r, "jdbc-coordination-ttl");
                t.setDaemon(true);
                return t;
            });
            ttlExecutor.scheduleAtFixedRate(this::deleteExpiredItems,
                    60, TTL_CLEANUP_INTERVAL.toSeconds(), TimeUnit.SECONDS);
        }
    }

    HikariDataSource createDataSource(final HikariConfig hikariConfig) {
        return new HikariDataSource(hikariConfig);
    }

    private void createTable() {
        final String tableName = settings.getTableName();
        final String createTableSql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "source_identifier VARCHAR(256) NOT NULL, "
                + "source_partition_key VARCHAR(256) NOT NULL, "
                + "partition_owner VARCHAR(256), "
                + "partition_progress_state TEXT, "
                + "source_partition_status VARCHAR(20) NOT NULL, "
                + "partition_ownership_timeout TIMESTAMP, "
                + "reopen_at TIMESTAMP, "
                + "closed_count BIGINT DEFAULT 0, "
                + "partition_priority VARCHAR(64), "
                + "version BIGINT NOT NULL DEFAULT 0, "
                + "expiration_time TIMESTAMP, "
                + "PRIMARY KEY (source_identifier, source_partition_key))";

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
                stmt.execute();
            }
            // CREATE INDEX IF NOT EXISTS is not supported by MySQL.
            // Use plain CREATE INDEX and ignore the error if the index already exists.
            try (PreparedStatement stmt = conn.prepareStatement(
                    "CREATE INDEX idx_source_status_priority ON "
                            + tableName + " (source_identifier, source_partition_status, partition_priority)")) {
                stmt.execute();
            } catch (final SQLException e) {
                if (!isIndexAlreadyExists(e)) {
                    throw e;
                }
                LOG.debug("Index already exists, skipping creation");
            }
            LOG.info("JDBC coordination store table {} initialized", tableName);
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to create coordination store table", e);
        }
    }

    @Override
    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourceIdentifier,
                                                                     final String sourcePartitionKey) {
        final String sql = "SELECT * FROM " + settings.getTableName()
                + " WHERE source_identifier = ? AND source_partition_key = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, sourceIdentifier);
            stmt.setString(2, sourcePartitionKey);
            try (ResultSet rs = stmt.executeQuery()) {
                final List<SourcePartitionStoreItem> items = mapResultSetToList(rs);
                return items.isEmpty() ? Optional.empty() : Optional.of(items.get(0));
            }
        } catch (final SQLException e) {
            LOG.error("Failed to get partition item for {} / {}", sourceIdentifier, sourcePartitionKey, e);
            return Optional.empty();
        }
    }

    @Override
    public List<SourcePartitionStoreItem> querySourcePartitionItemsByStatus(final String sourceIdentifier,
                                                                            final SourcePartitionStatus status,
                                                                            final String startPartitionPriority) {
        final String sql = "SELECT * FROM " + settings.getTableName()
                + " WHERE source_identifier = ? AND source_partition_status = ?"
                + " AND partition_priority >= ?"
                + " ORDER BY partition_priority";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, sourceIdentifier);
            stmt.setString(2, status.name());
            stmt.setString(3, startPartitionPriority);
            try (ResultSet rs = stmt.executeQuery()) {
                return mapResultSetToList(rs);
            }
        } catch (final SQLException e) {
            LOG.error("Failed to query partitions by status for {}", sourceIdentifier, e);
            return List.of();
        }
    }

    @Override
    public List<SourcePartitionStoreItem> queryAllSourcePartitionItems(final String sourceIdentifier) {
        final String sql = "SELECT * FROM " + settings.getTableName()
                + " WHERE source_identifier = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, sourceIdentifier);
            try (ResultSet rs = stmt.executeQuery()) {
                return mapResultSetToList(rs);
            }
        } catch (final SQLException e) {
            LOG.error("Failed to query all partitions for {}", sourceIdentifier, e);
            return List.of();
        }
    }

    @Override
    public boolean tryCreatePartitionItem(final String sourceIdentifier,
                                          final String sourcePartitionKey,
                                          final SourcePartitionStatus sourcePartitionStatus,
                                          final Long closedCount,
                                          final String partitionProgressState,
                                          final boolean isReadOnlyItem) {
        final String sql = "INSERT INTO " + settings.getTableName()
                + " (source_identifier, source_partition_key, source_partition_status,"
                + " closed_count, partition_progress_state, partition_priority, version, expiration_time)"
                + " VALUES (?, ?, ?, ?, ?, ?, 0, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, sourceIdentifier);
            stmt.setString(2, sourcePartitionKey);
            stmt.setString(3, sourcePartitionStatus.name());
            stmt.setLong(4, closedCount);
            stmt.setString(5, partitionProgressState);
            stmt.setString(6, Instant.now().toString());
            if (!isReadOnlyItem && settings.getTtl() != null) {
                stmt.setObject(7, LocalDateTime.ofInstant(
                        Instant.now().plus(settings.getTtl()), ZoneOffset.UTC));
            } else {
                stmt.setNull(7, Types.TIMESTAMP);
            }
            stmt.executeUpdate();
            return true;
        } catch (final SQLException e) {
            if (isConstraintViolation(e)) {
                return false;
            }
            LOG.error("Failed to create partition item {} / {}", sourceIdentifier, sourcePartitionKey, e);
            return false;
        }
    }

    @Override
    public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String sourceIdentifier,
                                                                           final String ownerId,
                                                                           final Duration ownershipTimeout) {
        // Step 1: ASSIGNED with expired ownership
        final LocalDateTime now = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        final Optional<SourcePartitionStoreItem> assigned = queryAndAcquire(sourceIdentifier, ownerId, ownershipTimeout,
                "SELECT * FROM " + settings.getTableName()
                        + " WHERE source_identifier = ? AND source_partition_status = 'ASSIGNED'"
                        + " AND partition_ownership_timeout < ?"
                        + " ORDER BY partition_priority LIMIT 1", now);
        if (assigned.isPresent()) {
            return assigned;
        }

        // Step 2: UNASSIGNED
        final Optional<SourcePartitionStoreItem> unassigned = queryAndAcquire(sourceIdentifier, ownerId, ownershipTimeout,
                "SELECT * FROM " + settings.getTableName()
                        + " WHERE source_identifier = ? AND source_partition_status = 'UNASSIGNED'"
                        + " ORDER BY partition_priority LIMIT 5", null);
        if (unassigned.isPresent()) {
            return unassigned;
        }

        // Step 3: CLOSED with expired reopen_at
        return queryAndAcquire(sourceIdentifier, ownerId, ownershipTimeout,
                "SELECT * FROM " + settings.getTableName()
                        + " WHERE source_identifier = ? AND source_partition_status = 'CLOSED'"
                        + " AND reopen_at < ?"
                        + " ORDER BY partition_priority LIMIT 1", now);
    }

    private Optional<SourcePartitionStoreItem> queryAndAcquire(final String sourceIdentifier,
                                                                final String ownerId,
                                                                final Duration ownershipTimeout,
                                                                final String querySql,
                                                                final LocalDateTime timeParam) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(querySql)) {
            stmt.setString(1, sourceIdentifier);
            if (timeParam != null) {
                stmt.setObject(2, timeParam);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    final JdbcPartitionItem item = mapResultSet(rs);
                    final Instant timeout = Instant.now().plus(ownershipTimeout);
                    item.setPartitionOwner(ownerId);
                    item.setPartitionOwnershipTimeout(timeout);
                    item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
                    item.setPartitionPriority(timeout.toString());
                    try {
                        tryUpdateItem(item);
                        return Optional.of(item);
                    } catch (final PartitionUpdateException e) {
                        // Another node acquired this row, try next
                    }
                }
            }
        } catch (final SQLException e) {
            LOG.error("Failed to acquire partition for {}", sourceIdentifier, e);
        }
        return Optional.empty();
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        tryUpdateSourcePartitionItemInternal(updateItem, null);
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem,
                                             final Instant priorityForUnassignedPartitions) {
        tryUpdateSourcePartitionItemInternal(updateItem, priorityForUnassignedPartitions);
    }

    private void tryUpdateSourcePartitionItemInternal(final SourcePartitionStoreItem updateItem,
                                                      final Instant priorityOverride) {
        final JdbcPartitionItem item = (JdbcPartitionItem) updateItem;

        if (SourcePartitionStatus.CLOSED.equals(item.getSourcePartitionStatus())) {
            item.setPartitionPriority(item.getReOpenAt() != null ? item.getReOpenAt().toString() : null);
        }
        if (SourcePartitionStatus.ASSIGNED.equals(item.getSourcePartitionStatus())) {
            item.setPartitionPriority(item.getPartitionOwnershipTimeout() != null
                    ? item.getPartitionOwnershipTimeout().toString() : null);
        }
        if (priorityOverride != null && SourcePartitionStatus.UNASSIGNED.equals(item.getSourcePartitionStatus())) {
            item.setPartitionPriority(priorityOverride.toString());
        }

        tryUpdateItem(item);
    }

    private void tryUpdateItem(final JdbcPartitionItem item) {
        final String sql = "UPDATE " + settings.getTableName()
                + " SET partition_owner = ?, partition_progress_state = ?,"
                + " source_partition_status = ?, partition_ownership_timeout = ?,"
                + " reopen_at = ?, closed_count = ?, partition_priority = ?,"
                + " version = version + 1, expiration_time = ?"
                + " WHERE source_identifier = ? AND source_partition_key = ? AND version = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int idx = 1;
            stmt.setString(idx++, item.getPartitionOwner());
            stmt.setString(idx++, item.getPartitionProgressState());
            stmt.setString(idx++, item.getSourcePartitionStatus().name());
            setInstant(stmt, idx++, item.getPartitionOwnershipTimeout());
            setInstant(stmt, idx++, item.getReOpenAt());
            stmt.setLong(idx++, item.getClosedCount() != null ? item.getClosedCount() : 0);
            stmt.setString(idx++, item.getPartitionPriority());
            if (settings.getTtl() != null) {
                setInstant(stmt, idx++, Instant.now().plus(settings.getTtl()));
            } else {
                setInstant(stmt, idx++, null);
            }
            stmt.setString(idx++, item.getSourceIdentifier());
            stmt.setString(idx++, item.getSourcePartitionKey());
            stmt.setLong(idx++, item.getVersion());

            final int updated = stmt.executeUpdate();
            if (updated == 0) {
                throw new PartitionUpdateException(
                        String.format("Failed to update partition %s. Version mismatch (expected %d).",
                                item.getSourcePartitionKey(), item.getVersion()), null);
            }
            item.setVersion(item.getVersion() + 1);
        } catch (final PartitionUpdateException e) {
            throw e;
        } catch (final SQLException e) {
            throw new PartitionUpdateException(
                    String.format("Failed to update partition %s", item.getSourcePartitionKey()), e);
        }
    }

    @Override
    public void tryDeletePartitionItem(final SourcePartitionStoreItem deleteItem) {
        final JdbcPartitionItem item = (JdbcPartitionItem) deleteItem;
        final String sql = "DELETE FROM " + settings.getTableName()
                + " WHERE source_identifier = ? AND source_partition_key = ? AND version = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, item.getSourceIdentifier());
            stmt.setString(2, item.getSourcePartitionKey());
            stmt.setLong(3, item.getVersion());
            final int deleted = stmt.executeUpdate();
            if (deleted == 0) {
                throw new PartitionUpdateException(
                        String.format("Failed to delete partition %s. Version mismatch.", item.getSourcePartitionKey()), null);
            }
        } catch (final PartitionUpdateException e) {
            throw e;
        } catch (final SQLException e) {
            throw new PartitionUpdateException(
                    String.format("Failed to delete partition %s", item.getSourcePartitionKey()), e);
        }
    }

    @Override
    public void close() {
        if (ttlExecutor != null) {
            ttlExecutor.shutdownNow();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    void deleteExpiredItems() {
        final String sql = "DELETE FROM " + settings.getTableName()
                + " WHERE expiration_time < ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            final int deleted = stmt.executeUpdate();
            if (deleted > 0) {
                LOG.debug("Deleted {} expired partition items", deleted);
            }
        } catch (final SQLException e) {
            LOG.warn("Failed to delete expired partition items", e);
        }
    }

    private JdbcPartitionItem mapResultSet(final ResultSet rs) throws SQLException {
        final JdbcPartitionItem item = new JdbcPartitionItem();
        item.setSourceIdentifier(rs.getString("source_identifier"));
        item.setSourcePartitionKey(rs.getString("source_partition_key"));
        item.setPartitionOwner(rs.getString("partition_owner"));
        item.setPartitionProgressState(rs.getString("partition_progress_state"));
        item.setSourcePartitionStatus(SourcePartitionStatus.valueOf(rs.getString("source_partition_status")));
        item.setPartitionOwnershipTimeout(getInstant(rs, "partition_ownership_timeout"));
        item.setReOpenAt(getInstant(rs, "reopen_at"));
        item.setClosedCount(rs.getLong("closed_count"));
        item.setPartitionPriority(rs.getString("partition_priority"));
        item.setVersion(rs.getLong("version"));
        return item;
    }

    private List<SourcePartitionStoreItem> mapResultSetToList(final ResultSet rs) throws SQLException {
        final List<SourcePartitionStoreItem> items = new ArrayList<>();
        while (rs.next()) {
            items.add(mapResultSet(rs));
        }
        return items;
    }

    private static void setInstant(final PreparedStatement stmt, final int index, final Instant instant)
            throws SQLException {
        if (instant != null) {
            stmt.setObject(index, LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        } else {
            stmt.setNull(index, Types.TIMESTAMP);
        }
    }

    private static Instant getInstant(final ResultSet rs, final String column) throws SQLException {
        final LocalDateTime ldt = rs.getObject(column, LocalDateTime.class);
        return ldt != null ? ldt.toInstant(ZoneOffset.UTC) : null;
    }

    // PostgreSQL: SQLSTATE 42P07 (duplicate_table, also covers indexes/relations)
    // MySQL: Error 1061 (ER_DUP_KEYNAME)
    private static boolean isIndexAlreadyExists(final SQLException ex) {
        return DUPLICATE_RELATION_SQLSTATE.equals(ex.getSQLState())
                || ex.getErrorCode() == MYSQL_DUPLICATE_KEY_NAME_ERROR;
    }

    // MySQL throws SQLIntegrityConstraintViolationException (JDBC standard) on PK violation.
    // PostgreSQL does not throw that subclass, but sets SQLSTATE 23505 (unique_violation,
    // defined in ISO/IEC 9075). Both cases indicate the row already exists.
    private static boolean isConstraintViolation(final SQLException ex) {
        return ex instanceof SQLIntegrityConstraintViolationException
                || UNIQUE_VIOLATION_SQLSTATE.equals(ex.getSQLState());
    }
}
