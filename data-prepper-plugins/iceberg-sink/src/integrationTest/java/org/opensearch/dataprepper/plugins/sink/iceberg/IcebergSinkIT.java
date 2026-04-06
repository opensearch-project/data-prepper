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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory.InMemorySourceCoordinationStore;
import org.opensearch.dataprepper.core.sourcecoordination.enhanced.EnhancedLeaseBasedSourceCoordinator;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.PartitionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for the Iceberg Sink plugin.
 * Requires Docker containers running:
 *   docker compose -f data-prepper-plugins/iceberg-source/docker/docker-compose.yml up -d
 */
public class IcebergSinkIT {

    private static final EventFactory EVENT_FACTORY = TestEventFactory.getTestEventFactory();
    private static final String REST_URI = System.getProperty("tests.iceberg.rest.uri", "http://localhost:8181");
    private static final String S3_ENDPOINT = System.getProperty("tests.iceberg.s3.endpoint", "http://localhost:8333");
    private static final String ACCESS_KEY = System.getProperty("tests.iceberg.s3.accessKey", "admin");
    private static final String SECRET_KEY = System.getProperty("tests.iceberg.s3.secretKey", "password");
    private static final String REGION = System.getProperty("tests.iceberg.s3.region", "us-east-1");

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get())
    );

    private RESTCatalog catalog;
    private String namespace;

    @BeforeEach
    void setUp() {
        catalog = new RESTCatalog();
        catalog.initialize("test", catalogProperties());
        namespace = "test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        catalog.createNamespace(Namespace.of(namespace));
    }

    @AfterEach
    void tearDown() {
        try {
            catalog.listTables(Namespace.of(namespace)).forEach(id -> catalog.dropTable(id, true));
            catalog.dropNamespace(Namespace.of(namespace));
            catalog.close();
        } catch (final Exception e) {
            // ignore
        }
    }

    @Test
    void appendOnly_writesRecordsToTable() throws Exception {
        final String tableName = "append_test";
        catalog.createTable(TableIdentifier.of(namespace, tableName), TEST_SCHEMA);

        final IcebergSinkConfig config = createConfig(namespace + "." + tableName, null);
        final RecordConverter converter = new RecordConverter(TEST_SCHEMA);
        final TaskWriterManager writerManager = new TaskWriterManager(
                catalog.loadTable(TableIdentifier.of(namespace, tableName)), config);

        // Write 3 records
        for (int i = 0; i < 3; i++) {
            final Map<String, Object> data = Map.of("id", i, "name", "user" + i, "age", 20 + i);
            writerManager.write(converter.convert(data));
        }

        // Flush and commit directly (no coordination store in this test)
        final org.apache.iceberg.io.WriteResult result = writerManager.flush();
        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        final org.apache.iceberg.AppendFiles append = table.newAppend();
        java.util.Arrays.stream(result.dataFiles()).forEach(append::appendFile);
        append.commit();

        // Verify
        final List<Record> records = readAll(table);
        assertEquals(3, records.size());
    }

    @Test
    void cdc_deleteRemovesRecord() throws Exception {
        final String tableName = "cdc_test";
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );
        catalog.createTable(TableIdentifier.of(namespace, tableName), schema);

        final IcebergSinkConfig config = createConfig(namespace + "." + tableName, "op");
        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        final RecordConverter converter = new RecordConverter(schema);
        final TaskWriterManager writerManager = new TaskWriterManager(table, config);

        // INSERT id=1
        writerManager.write(converter.convert(Map.of("id", 1, "name", "alice")), CdcOperation.INSERT);
        // INSERT id=2
        writerManager.write(converter.convert(Map.of("id", 2, "name", "bob")), CdcOperation.INSERT);

        // Flush and commit first batch
        org.apache.iceberg.io.WriteResult result1 = writerManager.flush();
        org.apache.iceberg.RowDelta rowDelta1 = table.newRowDelta();
        java.util.Arrays.stream(result1.dataFiles()).forEach(rowDelta1::addRows);
        java.util.Arrays.stream(result1.deleteFiles()).forEach(rowDelta1::addDeletes);
        rowDelta1.commit();

        // DELETE id=1
        writerManager.write(converter.convert(Map.of("id", 1, "name", "alice")), CdcOperation.DELETE);

        // Flush and commit second batch
        org.apache.iceberg.io.WriteResult result2 = writerManager.flush();
        table.refresh();
        org.apache.iceberg.RowDelta rowDelta2 = table.newRowDelta();
        java.util.Arrays.stream(result2.dataFiles()).forEach(rowDelta2::addRows);
        java.util.Arrays.stream(result2.deleteFiles()).forEach(rowDelta2::addDeletes);
        rowDelta2.commit();

        // Verify: only id=2 remains
        table.refresh();
        final List<Record> records = readAll(table);
        assertEquals(1, records.size());
        assertEquals(2, records.get(0).getField("id"));
    }

    @Test
    void autoCreate_createsTableFromSchemaDefinition() throws Exception {
        final String tableName = "auto_create_test";
        final Map<String, Object> schemaConfig = Map.of(
                "columns", List.of(
                        Map.of("name", "id", "type", "int", "required", true),
                        Map.of("name", "name", "type", "string"),
                        Map.of("name", "ts", "type", "timestamp")
                ),
                "partition_spec", List.of(
                        Map.of("column", "ts", "transform", "day")
                )
        );

        final IcebergSinkConfig config = createConfig(namespace + "." + tableName, null, true, schemaConfig);
        final IcebergSinkService service = new IcebergSinkService(config, createInMemoryCoordinator(), null, null, PluginMetrics.fromNames("iceberg", "test"));

        // Table should now exist
        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        assertEquals(3, table.schema().columns().size());
        assertEquals(1, table.spec().fields().size());

        service.shutdown();
    }

    @Test
    void autoCreate_infersSchemaFromFirstEvent() throws Exception {
        final String tableName = "auto_infer_test";

        final IcebergSinkConfig config = createConfig(namespace + "." + tableName, null, true, null);
        final IcebergSinkService service = new IcebergSinkService(config, createInMemoryCoordinator(), null, null, PluginMetrics.fromNames("iceberg", "test"));

        final Event event = EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withEventType("event")
                .withData(Map.of("id", 1, "name", "alice", "active", true))
                .build();
        service.output(List.of(new org.opensearch.dataprepper.model.record.Record<>(event)));

        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        assertEquals(3, table.schema().columns().size());

        service.shutdown();
    }

    @Test
    void schemaEvolution_addsNewColumn() throws Exception {
        final String tableName = "schema_evo_test";
        final Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );
        catalog.createTable(TableIdentifier.of(namespace, tableName), initialSchema);

        final Map<String, Object> configMap = new java.util.HashMap<>();
        configMap.put("catalog", catalogProperties());
        configMap.put("table_identifier", namespace + "." + tableName);
        configMap.put("schema_evolution", true);
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        final IcebergSinkConfig config = mapper.convertValue(configMap, IcebergSinkConfig.class);
        final IcebergSinkService service = new IcebergSinkService(config, createInMemoryCoordinator(), null, null, PluginMetrics.fromNames("iceberg", "test"));

        // Send event with new field "age"
        final Event event = EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withEventType("event")
                .withData(Map.of("id", 1, "name", "alice", "age", 30))
                .build();
        service.output(List.of(new org.opensearch.dataprepper.model.record.Record<>(event)));

        // Verify schema has 3 columns
        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        assertEquals(3, table.schema().columns().size());
        assertEquals("age", table.schema().findField("age").name());

        service.shutdown();
    }

    @Test
    void partitioned_writesRecordsToCorrectPartitions() throws Exception {
        final String tableName = "partitioned_test";
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "category", Types.StringType.get())
        );
        final Table table = catalog.createTable(
                TableIdentifier.of(namespace, tableName), schema,
                org.apache.iceberg.PartitionSpec.builderFor(schema).identity("category").build());

        final IcebergSinkConfig config = createConfig(namespace + "." + tableName, null);
        final RecordConverter converter = new RecordConverter(schema);
        final TaskWriterManager writerManager = new TaskWriterManager(table, config);

        writerManager.write(converter.convert(Map.of("id", 1, "category", "A")));
        writerManager.write(converter.convert(Map.of("id", 2, "category", "B")));
        writerManager.write(converter.convert(Map.of("id", 3, "category", "A")));

        final org.apache.iceberg.io.WriteResult result = writerManager.flush();
        final org.apache.iceberg.AppendFiles append = table.newAppend();
        java.util.Arrays.stream(result.dataFiles()).forEach(append::appendFile);
        append.commit();

        table.refresh();
        final List<Record> records = readAll(table);
        assertEquals(3, records.size());
        // Should have 2 data files (one per partition)
        assertEquals(2, result.dataFiles().length);
    }

    @Test
    void commitScheduler_commitsViaCoordinationStore() throws Exception {
        final String tableName = "commit_scheduler_test";
        catalog.createTable(TableIdentifier.of(namespace, tableName), TEST_SCHEMA);

        // Use short intervals so flush and commit happen quickly
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("catalog", catalogProperties());
        configMap.put("table_identifier", namespace + "." + tableName);
        configMap.put("commit_interval", "PT1S");
        configMap.put("flush_interval", "PT1S");
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        final IcebergSinkConfig config = mapper.convertValue(configMap, IcebergSinkConfig.class);

        final EnhancedSourceCoordinator coordinator = createInMemoryCoordinator();
        final IcebergSinkService service = new IcebergSinkService(config, coordinator, null, null,
                PluginMetrics.fromNames("iceberg", "test"));

        // Write events
        for (int i = 0; i < 3; i++) {
            final Event event = EVENT_FACTORY.eventBuilder(EventBuilder.class)
                    .withEventType("event")
                    .withData(Map.of("id", i, "name", "user" + i, "age", 20 + i))
                    .build();
            service.output(List.of(new org.opensearch.dataprepper.model.record.Record<>(event)));
        }

        // Wait for flush_interval to elapse, then trigger another output to cause flush
        Thread.sleep(1500);
        final Event triggerEvent = EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withEventType("event")
                .withData(Map.of("id", 99, "name", "trigger", "age", 0))
                .build();
        service.output(List.of(new org.opensearch.dataprepper.model.record.Record<>(triggerEvent)));

        // Wait for CommitScheduler to commit
        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(30))
                .pollInterval(java.time.Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
                    table.refresh();
                    assertEquals(4, readAll(table).size());
                });

        service.shutdown();
    }

    @Test
    void autoCreate_withTableProperties() throws Exception {
        final String tableName = "auto_create_props_test";
        final Map<String, Object> schemaConfig = Map.of(
                "columns", List.of(
                        Map.of("name", "id", "type", "int", "required", true),
                        Map.of("name", "value", "type", "string")
                )
        );

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("catalog", catalogProperties());
        configMap.put("table_identifier", namespace + "." + tableName);
        configMap.put("auto_create", true);
        configMap.put("schema", schemaConfig);
        configMap.put("table_properties", Map.of("write.format.default", "orc"));
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        final IcebergSinkConfig config = mapper.convertValue(configMap, IcebergSinkConfig.class);
        final IcebergSinkService service = new IcebergSinkService(config, createInMemoryCoordinator(), null, null,
                PluginMetrics.fromNames("iceberg", "test"));

        final Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        assertEquals("orc", table.properties().get("write.format.default"));

        service.shutdown();
    }

    private EnhancedSourceCoordinator createInMemoryCoordinator() {
        final InMemorySourceCoordinationStore store = new InMemorySourceCoordinationStore(
                new org.opensearch.dataprepper.model.configuration.PluginSetting("in_memory", Collections.emptyMap()));
        final SourceCoordinationConfig coordinationConfig = new SourceCoordinationConfig(
                new PluginModel("in_memory", Collections.emptyMap()), null);
        final PluginMetrics coordinatorMetrics = PluginMetrics.fromNames("source-coordinator", "iceberg-sink-it");
        final EnhancedLeaseBasedSourceCoordinator coordinator = new EnhancedLeaseBasedSourceCoordinator(
                store, coordinationConfig, coordinatorMetrics, "iceberg-sink-it", new PartitionFactory());
        coordinator.initialize();
        return coordinator;
    }

    private List<Record> readAll(final Table table) {
        final List<Record> records = new ArrayList<>();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            iterable.forEach(records::add);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    private Map<String, String> catalogProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("type", "rest");
        props.put(CatalogProperties.URI, REST_URI);
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", S3_ENDPOINT);
        props.put("s3.access-key-id", ACCESS_KEY);
        props.put("s3.secret-access-key", SECRET_KEY);
        props.put("s3.path-style-access", "true");
        props.put("client.region", REGION);
        return props;
    }

    private IcebergSinkConfig createConfig(final String tableIdentifier, final String operation) {
        return createConfig(tableIdentifier, operation, false, null);
    }

    private IcebergSinkConfig createConfig(final String tableIdentifier, final String operation,
                                           final boolean autoCreate, final Map<String, Object> schemaConfig) {
        final Map<String, Object> map = new HashMap<>();
        map.put("catalog", catalogProperties());
        map.put("table_identifier", tableIdentifier);
        map.put("auto_create", autoCreate);
        if (operation != null) {
            map.put("operation", operation);
            map.put("identifier_columns", List.of("id"));
        }
        if (schemaConfig != null) {
            map.put("schema", schemaConfig);
        }
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        return mapper.convertValue(map, IcebergSinkConfig.class);
    }
}
