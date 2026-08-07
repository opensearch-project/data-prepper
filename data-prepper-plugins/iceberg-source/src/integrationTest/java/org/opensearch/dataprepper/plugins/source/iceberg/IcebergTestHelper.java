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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Helper for integration tests. Uses Iceberg Java API to create tables and write
 * actual Parquet files, following the same pattern as Iceberg's own iceberg-data
 * test suite (TestWriterMetrics, TestPartitioningWriters, etc.).
 */
public class IcebergTestHelper {

    private final RESTCatalog catalog;
    private final String restUri;
    private final String s3Endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String region;

    public IcebergTestHelper(final String restUri,
                             final String s3Endpoint,
                             final String accessKey,
                             final String secretKey,
                             final String region) {
        this.restUri = restUri;
        this.s3Endpoint = s3Endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;

        this.catalog = new RESTCatalog();
        final Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, restUri);
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", s3Endpoint);
        props.put("s3.access-key-id", accessKey);
        props.put("s3.secret-access-key", secretKey);
        props.put("s3.path-style-access", "true");
        props.put("client.region", region);
        this.catalog.initialize("integration-test", props);
    }

    public RESTCatalog catalog() {
        return catalog;
    }

    public Map<String, String> catalogProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("type", "rest");
        props.put(CatalogProperties.URI, restUri);
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", s3Endpoint);
        props.put("s3.access-key-id", accessKey);
        props.put("s3.secret-access-key", secretKey);
        props.put("s3.path-style-access", "true");
        props.put("client.region", region);
        return props;
    }

    public void createNamespace(final String namespace) {
        catalog.createNamespace(Namespace.of(namespace));
    }

    public void dropNamespace(final String namespace) {
        try {
            catalog.dropNamespace(Namespace.of(namespace));
        } catch (final Exception e) {
            // ignore
        }
    }

    public Table createTable(final String namespace, final String tableName, final Schema schema) {
        return catalog.createTable(TableIdentifier.of(namespace, tableName), schema, PartitionSpec.unpartitioned());
    }

    public Table createPartitionedTable(final String namespace, final String tableName,
                                         final Schema schema, final PartitionSpec spec) {
        return catalog.createTable(TableIdentifier.of(namespace, tableName), schema, spec);
    }

    public void dropTable(final String namespace, final String tableName) {
        try {
            catalog.dropTable(TableIdentifier.of(namespace, tableName), true);
        } catch (final Exception e) {
            // ignore
        }
    }

    /**
     * Write records to a Parquet file and return the DataFile metadata.
     */
    public DataFile writeDataFile(final Table table, final List<Record> records) throws IOException {
        final GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
        final String filePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        final OutputFile outputFile = table.io().newOutputFile(filePath);

        final DataWriter<Record> writer = appenderFactory.newDataWriter(
                PlaintextEncryptionManager.instance().encrypt(outputFile), FileFormat.PARQUET, null);
        try (writer) {
            for (final Record record : records) {
                writer.write(record);
            }
        }
        return writer.toDataFile();
    }

    /**
     * Append records as a new snapshot.
     */
    public DataFile appendRows(final Table table, final List<Record> records) throws IOException {
        final DataFile dataFile = writeDataFile(table, records);
        table.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    /**
     * Simulate a CoW UPDATE or DELETE: remove old data file, add new data file.
     * This is how CoW works: the entire data file is rewritten.
     */
    public DataFile overwriteRows(final Table table, final DataFile oldDataFile,
                                  final List<Record> newRecords) throws IOException {
        final DataFile newDataFile = writeDataFile(table, newRecords);
        table.newOverwrite()
                .deleteFile(oldDataFile)
                .addFile(newDataFile)
                .commit();
        return newDataFile;
    }

    public GenericRecord newRecord(final Schema schema, final Object... values) {
        final GenericRecord record = GenericRecord.create(schema);
        final List<org.apache.iceberg.types.Types.NestedField> fields = schema.columns();
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        return record;
    }

    public void close() {
        try {
            catalog.close();
        } catch (final Exception e) {
            // ignore
        }
    }
}
