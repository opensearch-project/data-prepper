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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class TaskWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(TaskWriterManager.class);

    private final Table table;
    private final Duration flushInterval;
    private final boolean cdcEnabled;
    private final Set<Integer> identifierFieldIds;

    private TaskWriter<Record> writer;
    private Instant lastFlushTime;

    public TaskWriterManager(final Table table, final IcebergSinkConfig config) {
        this.table = table;
        this.flushInterval = config.getFlushInterval();
        this.cdcEnabled = config.getOperation() != null;
        this.identifierFieldIds = resolveIdentifierFieldIds(table, config);
        this.writer = createWriter();
        this.lastFlushTime = Instant.now();
    }

    public void write(final Record record, final CdcOperation operation) throws IOException {
        if (!cdcEnabled || operation == CdcOperation.INSERT) {
            writer.write(record);
            return;
        }
        if (!(writer instanceof DeltaTaskWriter)) {
            writer.write(record);
            return;
        }
        final DeltaTaskWriter deltaWriter = (DeltaTaskWriter) writer;
        if (operation == CdcOperation.DELETE) {
            deltaWriter.writeDelete(record);
        } else if (operation == CdcOperation.UPDATE) {
            deltaWriter.writeDelete(record);
            deltaWriter.write(record);
        }
    }

    public void write(final Record record) throws IOException {
        write(record, CdcOperation.INSERT);
    }

    public WriteResult flushIfNeeded() throws IOException {
        if (shouldFlush()) {
            return flush();
        }
        return null;
    }

    public WriteResult flush() throws IOException {
        final WriteResult result = writer.complete();
        writer.close();
        writer = createWriter();
        lastFlushTime = Instant.now();
        LOG.debug("Flushed TaskWriter: {} data files, {} delete files",
                result.dataFiles().length, result.deleteFiles().length);
        return result;
    }

    public void close() throws IOException {
        writer.close();
    }

    private boolean shouldFlush() {
        return Duration.between(lastFlushTime, Instant.now()).compareTo(flushInterval) >= 0;
    }

    /**
     * Selects the appropriate TaskWriter based on table configuration:
     * 1. CDC enabled (operation configured + identifier columns present) -> DeltaTaskWriter
     *    Handles INSERT/UPDATE/DELETE with equality deletes. Manages partitions internally.
     * 2. Append-only + unpartitioned -> UnpartitionedWriter
     *    Simplest writer. Single data file, rotates on targetFileSize.
     * 3. Append-only + partitioned -> PartitionedFanoutWriter
     *    Opens one data file per partition simultaneously (fanout).
     */
    private TaskWriter<Record> createWriter() {
        final FileFormat format = FileFormat.valueOf(
                table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                        .toUpperCase());
        final long targetFileSize = PropertyUtil.propertyAsLong(
                table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
        final Schema schema = table.schema();
        final PartitionSpec spec = table.spec();

        final int[] idFieldIds = identifierFieldIds.stream().mapToInt(Integer::intValue).toArray();
        final Schema deleteSchema = cdcEnabled && !identifierFieldIds.isEmpty()
                ? TypeUtil.select(schema, new HashSet<>(identifierFieldIds))
                : null;

        final GenericAppenderFactory appenderFactory = deleteSchema != null
                ? new GenericAppenderFactory(schema, spec, idFieldIds, deleteSchema, null).setAll(table.properties())
                : new GenericAppenderFactory(schema, spec).setAll(table.properties());

        final OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(spec)
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();

        if (cdcEnabled && deleteSchema != null) {
            return new DeltaTaskWriter(spec, format, appenderFactory, fileFactory, table.io(),
                    targetFileSize, schema, deleteSchema);
        }

        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize);
        }

        final PartitionKey pk = new PartitionKey(spec, schema);
        final InternalRecordWrapper w = new InternalRecordWrapper(schema.asStruct());
        return new PartitionedFanoutWriter<Record>(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize) {
            @Override
            protected PartitionKey partition(final Record record) {
                pk.partition(w.wrap(record));
                return pk;
            }
        };
    }

    private static Set<Integer> resolveIdentifierFieldIds(final Table table, final IcebergSinkConfig config) {
        if (!config.getIdentifierColumns().isEmpty()) {
            return config.getIdentifierColumns().stream()
                    .map(name -> table.schema().findField(name).fieldId())
                    .collect(Collectors.toSet());
        }
        return table.schema().identifierFieldIds();
    }

    /**
     * Delta writer that supports INSERT, DELETE, UPDATE using BaseEqualityDeltaWriter.
     * Maintains a map of EqDeltaWriter per partition for partitioned tables.
     */
    static class DeltaTaskWriter extends BaseTaskWriter<Record> {
        private final Schema schema;
        private final Schema deleteSchema;
        private final InternalRecordWrapper wrapper;
        private final InternalRecordWrapper keyWrapper;
        private final Map<Object, EqDeltaWriter> writers = new HashMap<>();
        private final boolean partitioned;
        private final PartitionKey partitionKey;

        DeltaTaskWriter(PartitionSpec spec, FileFormat format,
                        FileAppenderFactory<Record> appenderFactory, OutputFileFactory fileFactory,
                        FileIO io, long targetFileSize, Schema schema, Schema deleteSchema) {
            super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
            this.schema = schema;
            this.deleteSchema = deleteSchema;
            this.wrapper = new InternalRecordWrapper(schema.asStruct());
            this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
            this.partitioned = !spec.isUnpartitioned();
            this.partitionKey = partitioned ? new PartitionKey(spec, schema) : null;
        }

        @Override
        public void write(final Record record) throws IOException {
            route(record).write(record);
        }

        void writeDelete(final Record record) throws IOException {
            final GenericRecord keyRecord = GenericRecord.create(deleteSchema);
            for (final Types.NestedField field : deleteSchema.columns()) {
                keyRecord.setField(field.name(), record.getField(field.name()));
            }
            route(record).deleteKey(keyRecord);
        }

        /**
         * Routes a record to the EqDeltaWriter for its partition.
         * Unpartitioned tables use a single writer keyed by null.
         * Partitioned tables compute the partition key and maintain one writer per partition.
         */
        private EqDeltaWriter route(final Record record) {
            if (!partitioned) {
                return writers.computeIfAbsent(null, k -> new EqDeltaWriter(null));
            }
            partitionKey.partition(wrapper.wrap(record));
            final PartitionKey copy = partitionKey.copy();
            return writers.computeIfAbsent(copy, k -> new EqDeltaWriter(copy));
        }

        @Override
        public void close() throws IOException {
            for (final EqDeltaWriter w : writers.values()) {
                w.close();
            }
            writers.clear();
        }

        class EqDeltaWriter extends BaseEqualityDeltaWriter {
            EqDeltaWriter(final PartitionKey partition) {
                super(partition, schema, deleteSchema);
            }

            @Override
            protected StructLike asStructLike(final Record data) {
                return wrapper.wrap(data);
            }

            @Override
            protected StructLike asStructLikeKey(final Record data) {
                return keyWrapper.wrap(data);
            }
        }
    }
}
