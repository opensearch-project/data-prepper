/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import io.micrometer.core.instrument.Counter;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Type;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.WriteResultPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.state.WriteResultState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkService.class);

    static final String RECORDS_SUCCEEDED = "icebergSinkRecordsSucceeded";
    static final String RECORDS_FAILED = "icebergSinkRecordsFailed";
    static final String FLUSH_COUNT = "icebergSinkFlushCount";
    static final String COMMIT_COUNT = "icebergSinkCommitCount";

    private final IcebergSinkConfig config;
    private final Catalog catalog;
    private final EnhancedSourceCoordinator coordinator;
    private final ExpressionEvaluator expressionEvaluator;
    private final DlqPushHandler dlqPushHandler;
    private final Counter recordsSucceeded;
    private final Counter recordsFailed;
    private final Counter flushCount;
    private final Counter commitCount;
    private final boolean dynamicRouting;
    private final ConcurrentHashMap<String, TableContext> tableContexts;
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, TaskWriterManager>> writersByThread;
    private final ConcurrentHashMap<Long, List<EventHandle>> currentEventHandles;
    private final ConcurrentHashMap<String, List<EventHandle>> pendingEventHandles;
    private final ExecutorService commitSchedulerExecutor;
    private final ScheduledExecutorService ackPollExecutor;
    private volatile Instant lastAckPollTime;

    public IcebergSinkService(final IcebergSinkConfig config,
                              final EnhancedSourceCoordinator coordinator,
                              final ExpressionEvaluator expressionEvaluator,
                              final DlqPushHandler dlqPushHandler,
                              final PluginMetrics pluginMetrics) {
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
        this.dlqPushHandler = dlqPushHandler;
        this.recordsSucceeded = pluginMetrics.counter(RECORDS_SUCCEEDED);
        this.recordsFailed = pluginMetrics.counter(RECORDS_FAILED);
        this.flushCount = pluginMetrics.counter(FLUSH_COUNT);
        this.commitCount = pluginMetrics.counter(COMMIT_COUNT);

        if (coordinator == null) {
            throw new IllegalStateException(
                    "Iceberg sink requires source_coordination to be configured in data-prepper-config.yaml.");
        }
        this.coordinator = coordinator;

        this.catalog = CatalogUtil.buildIcebergCatalog("iceberg-sink", config.getCatalog(), null);
        this.dynamicRouting = config.getTableIdentifier().contains("${");
        this.tableContexts = new ConcurrentHashMap<>();
        this.writersByThread = new ConcurrentHashMap<>();
        this.currentEventHandles = new ConcurrentHashMap<>();
        this.pendingEventHandles = new ConcurrentHashMap<>();

        // Validate: dynamic routing + schema definition is not allowed
        if (dynamicRouting && config.getSchemaConfig() != null) {
            throw new IllegalArgumentException(
                    "schema definition cannot be used with dynamic table_identifier");
        }

        // For static routing, load/create table eagerly
        if (!dynamicRouting) {
            getOrCreateTableContext(config.getTableIdentifier(), null);
            validateIdentifierColumns(config);
        }

        // Start CommitScheduler
        coordinator.createPartition(new LeaderPartition());
        commitSchedulerExecutor = Executors.newSingleThreadExecutor();
        final CommitScheduler commitScheduler = new CommitScheduler(
                coordinator, catalog, config.getCommitInterval(), commitCount);
        commitSchedulerExecutor.submit(commitScheduler);

        // Start ack polling
        this.lastAckPollTime = Instant.now();
        this.ackPollExecutor = Executors.newSingleThreadScheduledExecutor();
        final long pollMillis = config.getAckPollInterval().toMillis();
        ackPollExecutor.scheduleAtFixedRate(this::releaseCommittedEventHandles,
                pollMillis, pollMillis, TimeUnit.MILLISECONDS);

        LOG.info("Initialized IcebergSinkService, dynamicRouting={}", dynamicRouting);
    }

    /**
     * Processes a batch of events. For each event:
     * 1. Resolve table: evaluate table_identifier expression (static or dynamic routing)
     * 2. Get or create TableContext: load table from catalog, or auto-create if configured
     * 3. Schema evolution: if enabled, add new columns for fields not in the current schema
     * 4. Convert: Event Map -> Iceberg GenericRecord using the table's schema
     * 5. Resolve operation: evaluate operation expression -> CdcOperation (INSERT/UPDATE/DELETE)
     * 6. Write: pass record and operation to TaskWriterManager
     * On error, the event is sent to DLQ (if configured) and the EventHandle is released as failed.
     */
    public void output(final Collection<Record<Event>> records) {
        final long threadId = Thread.currentThread().getId();
        final ConcurrentHashMap<String, TaskWriterManager> threadWriters =
                writersByThread.computeIfAbsent(threadId, id -> new ConcurrentHashMap<>());

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            try {
                final String resolvedTable = resolveTableIdentifier(event);
                final TableContext ctx = getOrCreateTableContext(resolvedTable, event);
                if (ctx == null) {
                    LOG.error("Failed to resolve table for event, sending to DLQ");
                    sendToDlq(event, "Failed to resolve table");
                    recordsFailed.increment();
                    releaseEventHandle(event, false);
                    continue;
                }

                TaskWriterManager writerManager = threadWriters.computeIfAbsent(
                        resolvedTable, t -> new TaskWriterManager(ctx.table, config));

                // Recreate writer if schema has changed (e.g. by another thread's evolveSchema)
                if (writerManager.schemaId() != ctx.table.schema().schemaId()) {
                    final WriteResult staleResult = writerManager.flush();
                    if (staleResult.dataFiles().length > 0 || staleResult.deleteFiles().length > 0) {
                        registerWriteResult(resolvedTable, staleResult);
                    }
                    writerManager = new TaskWriterManager(ctx.table, config);
                    threadWriters.put(resolvedTable, writerManager);
                }

                final Map<String, Object> data = event.toMap();

                // Schema evolution: detect new fields
                if (config.isSchemaEvolution() && hasNewFields(data, ctx)) {
                    evolveSchema(ctx, data, resolvedTable, threadWriters);
                }

                final GenericRecord icebergRecord = ctx.converter.convert(data);
                final CdcOperation operation = resolveOperation(event);
                if (operation == null) {
                    LOG.error("Unrecognized operation for event, sending to DLQ");
                    sendToDlq(event, "Unrecognized operation");
                    recordsFailed.increment();
                    releaseEventHandle(event, false);
                    continue;
                }
                writerManager.write(icebergRecord, operation);
                recordsSucceeded.increment();
                final EventHandle handle = event.getEventHandle();
                if (handle != null) {
                    currentEventHandles.computeIfAbsent(threadId, id -> new ArrayList<>()).add(handle);
                }
            } catch (final Exception e) {
                LOG.error("Failed to convert/write event, sending to DLQ", e);
                sendToDlq(event, "Failed to convert/write: " + e.getMessage());
                recordsFailed.increment();
                releaseEventHandle(event, false);
            }
        }

        // Check flush for all active writers on this thread
        for (final Map.Entry<String, TaskWriterManager> entry : threadWriters.entrySet()) {
            try {
                final WriteResult result = entry.getValue().flushIfNeeded();
                if (result != null) {
                    registerWriteResult(entry.getKey(), result);
                    flushCount.increment();
                }
            } catch (final IOException e) {
                LOG.error("Failed to flush for table {}", entry.getKey(), e);
            }
        }
    }

    public void shutdown() {
        writersByThread.forEach((threadId, threadWriters) ->
                threadWriters.forEach((tableName, writerManager) -> {
                    try {
                        final WriteResult result = writerManager.flush();
                        if (result.dataFiles().length > 0 || result.deleteFiles().length > 0) {
                            registerWriteResult(tableName, result);
                        }
                    } catch (final IOException e) {
                        LOG.warn("Failed to flush TaskWriter for table {}", tableName, e);
                    } finally {
                        try {
                            writerManager.close();
                        } catch (final IOException e) {
                            LOG.warn("Failed to close TaskWriter for table {}", tableName, e);
                        }
                    }
                }));

        if (ackPollExecutor != null) {
            ackPollExecutor.shutdownNow();
        }

        pendingEventHandles.values().forEach(handles ->
                handles.forEach(h -> h.release(false)));
        pendingEventHandles.clear();
        currentEventHandles.values().forEach(handles ->
                handles.forEach(h -> h.release(false)));
        currentEventHandles.clear();

        if (commitSchedulerExecutor != null) {
            commitSchedulerExecutor.shutdownNow();
        }

        if (catalog instanceof AutoCloseable) {
            try {
                ((AutoCloseable) catalog).close();
            } catch (final Exception e) {
                LOG.warn("Failed to close catalog", e);
            }
        }
    }

    private String resolveTableIdentifier(final Event event) {
        if (!dynamicRouting) {
            return config.getTableIdentifier();
        }
        return event.formatString(config.getTableIdentifier(), expressionEvaluator);
    }

    private TableContext getOrCreateTableContext(final String tableIdentifier, final Event event) {
        return tableContexts.computeIfAbsent(tableIdentifier, id -> {
            final TableIdentifier tableId = TableIdentifier.parse(id);
            Table table;
            try {
                table = catalog.loadTable(tableId);
            } catch (final NoSuchTableException e) {
                if (!config.isAutoCreate()) {
                    LOG.error("Table {} does not exist and auto_create is false", id);
                    return null;
                }
                table = createTable(tableId, event);
            }
            return table != null ? new TableContext(table) : null;
        });
    }

    private Table createTable(final TableIdentifier tableId, final Event event) {
        // Ensure namespace exists
        if (catalog instanceof SupportsNamespaces) {
            try {
                ((SupportsNamespaces) catalog).createNamespace(tableId.namespace());
            } catch (final AlreadyExistsException e) {
                // ignore
            }
        }

        final Schema schema;
        final PartitionSpec partitionSpec;
        if (config.getSchemaConfig() != null) {
            schema = config.getSchemaConfig().toIcebergSchema();
            partitionSpec = config.getSchemaConfig().toPartitionSpec(schema);
        } else if (event != null) {
            schema = SchemaInference.infer(event.toMap());
            partitionSpec = PartitionSpec.unpartitioned();
        } else {
            LOG.error("Cannot create table without schema or event for inference");
            return null;
        }

        try {
            final Map<String, String> props = config.getTableProperties();
            final String location = config.getTableLocation();
            Table created;
            if (!props.isEmpty() || location != null) {
                created = catalog.createTable(tableId, schema, partitionSpec, location, props);
            } else {
                created = catalog.createTable(tableId, schema, partitionSpec);
            }
            LOG.info("Created table: {}", tableId);
            return created;
        } catch (final AlreadyExistsException e) {
            LOG.info("Table {} already exists, loading", tableId);
            return catalog.loadTable(tableId);
        }
    }

    private void registerWriteResult(final String tableIdentifier, final WriteResult result) {
        final TableContext ctx = tableContexts.get(tableIdentifier);
        if (ctx == null) {
            return;
        }
        try {
            final WriteResultState state = ctx.deltaManifestWriter.write(tableIdentifier, result);
            final String partitionId = UUID.randomUUID().toString();
            final long threadId = Thread.currentThread().getId();
            final List<EventHandle> handles = currentEventHandles.remove(threadId);
            coordinator.createPartition(new WriteResultPartition(partitionId, state));
            if (handles != null && !handles.isEmpty()) {
                pendingEventHandles.put(partitionId, handles);
            }
        } catch (final IOException e) {
            LOG.error("Failed to write delta manifest for table {}", tableIdentifier, e);
        }
    }

    private void releaseEventHandle(final Event event, final boolean success) {
        final EventHandle handle = event.getEventHandle();
        if (handle != null) {
            handle.release(success);
        }
    }

    private void releaseCommittedEventHandles() {
        if (pendingEventHandles.isEmpty()) {
            return;
        }
        try {
            final Instant checkSince = lastAckPollTime;
            lastAckPollTime = Instant.now();
            final List<EnhancedSourcePartition> completed =
                    coordinator.queryCompletedPartitions(WriteResultPartition.PARTITION_TYPE, checkSince);
            for (final EnhancedSourcePartition partition : completed) {
                final String partitionKey = partition.getPartitionKey();
                final List<EventHandle> handles = pendingEventHandles.remove(partitionKey);
                if (handles != null) {
                    handles.forEach(h -> h.release(true));
                }
            }
        } catch (final Exception e) {
            LOG.error("Failed to poll for committed partitions", e);
        }
    }

    private void sendToDlq(final Event event, final String message) {
        if (dlqPushHandler == null) {
            return;
        }
        try {
            final DlqObject dlqObject = DlqObject.builder()
                    .withPluginName("iceberg")
                    .withPluginId("iceberg")
                    .withPipelineName(dlqPushHandler.getPluginSetting().getPipelineName())
                    .withFailedData(event.toJsonString())
                    .withTimestamp(Instant.now())
                    .build();
            dlqPushHandler.perform(Collections.singletonList(dlqObject));
        } catch (final Exception e) {
            LOG.error("Failed to send event to DLQ", e);
        }
    }

    private boolean hasNewFields(final Map<String, Object> data, final TableContext ctx) {
        final Schema schema = ctx.table.schema();
        for (final String key : data.keySet()) {
            if (schema.findField(key) == null) {
                return true;
            }
        }
        return false;
    }

    private void evolveSchema(final TableContext ctx, final Map<String, Object> data,
                              final String tableIdentifier,
                              final ConcurrentHashMap<String, TaskWriterManager> threadWriters) {
        // Flush current writer before schema change
        final TaskWriterManager writerManager = threadWriters.get(tableIdentifier);
        if (writerManager != null) {
            try {
                final WriteResult result = writerManager.flush();
                if (result.dataFiles().length > 0 || result.deleteFiles().length > 0) {
                    registerWriteResult(tableIdentifier, result);
                }
            } catch (final IOException e) {
                LOG.error("Failed to flush before schema evolution", e);
            }
        }

        // Add new columns
        final UpdateSchema updateSchema = ctx.table.updateSchema();
        boolean hasChanges = false;
        for (final Map.Entry<String, Object> entry : data.entrySet()) {
            if (ctx.table.schema().findField(entry.getKey()) == null) {
                final Type type = SchemaInference.inferType(entry.getValue(), new AtomicInteger(ctx.table.schema().highestFieldId() + 1));
                updateSchema.addColumn(entry.getKey(), type);
                hasChanges = true;
            }
        }

        if (hasChanges) {
            try {
                updateSchema.commit();
                LOG.info("Schema evolution: added new columns to table {}", tableIdentifier);
            } catch (final Exception e) {
                LOG.warn("Schema evolution failed (may have been done by another node), refreshing", e);
            }
            ctx.table.refresh();
            tableContexts.put(tableIdentifier, new TableContext(ctx.table));
            threadWriters.put(tableIdentifier, new TaskWriterManager(ctx.table, config));
        }
    }

    private void validateIdentifierColumns(final IcebergSinkConfig config) {
        if (config.getIdentifierColumns().isEmpty()) {
            return;
        }
        final TableContext ctx = tableContexts.get(config.getTableIdentifier());
        if (ctx == null) {
            return;
        }
        final Schema schema = ctx.table.schema();
        for (final String name : config.getIdentifierColumns()) {
            if (schema.findField(name) == null) {
                throw new IllegalArgumentException(
                        "identifier_columns contains unknown column '" + name + "' not found in table " + config.getTableIdentifier());
            }
        }
    }

    private CdcOperation resolveOperation(final Event event) {
        final String operationExpr = config.getOperation();
        if (operationExpr == null) {
            return CdcOperation.INSERT;
        }
        final String value = event.formatString(operationExpr, expressionEvaluator);
        return CdcOperation.from(value);
    }

    /**
     * Holds per-table state: loaded Table, RecordConverter, DeltaManifestWriter.
     */
    static class TableContext {
        final Table table;
        final RecordConverter converter;
        final DeltaManifestWriter deltaManifestWriter;

        TableContext(final Table table) {
            this.table = table;
            this.converter = new RecordConverter(table.schema());
            this.deltaManifestWriter = new DeltaManifestWriter(table);
        }
    }
}
