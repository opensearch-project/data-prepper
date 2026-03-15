/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IcebergService {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergService.class);
    private static final String COW_MODE = "copy-on-write";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final IcebergSourceConfig sourceConfig;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private ExecutorService executor;

    public IcebergService(final EnhancedSourceCoordinator sourceCoordinator,
                          final IcebergSourceConfig sourceConfig,
                          final PluginMetrics pluginMetrics,
                          final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
    }

    public void start(final Buffer<Record<Event>> buffer) {
        LOG.info("Starting Iceberg service");

        // Load all tables upfront. Single point of Table lifecycle management.
        final Map<String, Table> tables = new HashMap<>();
        final Map<String, TableConfig> tableConfigs = new HashMap<>();

        for (final TableConfig tableConfig : sourceConfig.getTables()) {
            final String tableName = tableConfig.getTableName();
            LOG.info("Loading catalog and table for {}", tableName);

            final Map<String, String> catalogProps = new HashMap<>(tableConfig.getCatalog());
            final Catalog catalog = CatalogUtil.buildIcebergCatalog(tableName, catalogProps, null);

            final TableIdentifier tableId = TableIdentifier.parse(tableName);

            final Table table = catalog.loadTable(tableId);
            validateCoWTable(table, tableName);

            if (tableConfig.getIdentifierColumns().isEmpty()) {
                LOG.warn("No identifier_columns configured for table {}. "
                        + "CDC correctness requires identifier_columns for UPDATE/DELETE support "
                        + "and idempotent writes.", tableName);
            } else {
                for (final String col : tableConfig.getIdentifierColumns()) {
                    if (table.schema().findField(col) == null) {
                        throw new IllegalArgumentException(
                                "identifier_columns contains '" + col + "' which does not exist in table " + tableName);
                    }
                }
            }

            tables.put(tableName, table);
            tableConfigs.put(tableName, tableConfig);

            LOG.info("Loaded table {} (current snapshot: {})",
                    tableName,
                    table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none");
        }

        // Start schedulers with shared table references
        final List<Runnable> runnableList = new ArrayList<>();

        runnableList.add(new LeaderScheduler(sourceCoordinator, tableConfigs, sourceConfig.getPollingInterval(), tables, pluginMetrics));
        runnableList.add(new ChangelogWorker(
                sourceCoordinator, sourceConfig, tables, tableConfigs, buffer, acknowledgementSetManager, pluginMetrics,
                new org.opensearch.dataprepper.plugins.source.iceberg.worker.IcebergDataFileReader()));

        executor = Executors.newFixedThreadPool(runnableList.size());
        runnableList.forEach(executor::submit);
    }

    public void shutdown() {
        LOG.info("Shutting down Iceberg service");
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void validateCoWTable(final Table table, final String tableName) {
        final String deleteMode = table.properties().getOrDefault("write.delete.mode", COW_MODE);
        final String updateMode = table.properties().getOrDefault("write.update.mode", COW_MODE);
        final String mergeMode = table.properties().getOrDefault("write.merge.mode", COW_MODE);

        if (!COW_MODE.equals(deleteMode) || !COW_MODE.equals(updateMode) || !COW_MODE.equals(mergeMode)) {
            throw new IllegalArgumentException(
                    "Table " + tableName + " uses Merge-on-Read (delete.mode=" + deleteMode
                            + ", update.mode=" + updateMode + ", merge.mode=" + mergeMode
                            + "). Only Copy-on-Write tables are supported.");
        }
    }
}
