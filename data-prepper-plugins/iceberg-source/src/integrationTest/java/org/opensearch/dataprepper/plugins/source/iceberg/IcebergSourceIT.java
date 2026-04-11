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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.core.sourcecoordination.enhanced.EnhancedLeaseBasedSourceCoordinator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory.InMemorySourceCoordinationStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for the Iceberg CDC source plugin.
 * <p>
 * Requires Docker containers running (see docker/docker-compose.yml):
 *   docker compose -f docker/docker-compose.yml up -d
 * <p>
 * Run with:
 *   ./gradlew :data-prepper-plugins:iceberg-source:integrationTest
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IcebergSourceIT {

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get())
    );

    private static final String TEST_NAMESPACE = "test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private static final String TEST_TABLE = "users";

    private IcebergTestHelper helper;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginFactory pluginFactory;

    private final List<org.opensearch.dataprepper.model.record.Record<Event>> receivedRecords =
            Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setUp() {
        final String restUri = System.getProperty("tests.iceberg.rest.uri", "http://localhost:8181");
        final String s3Endpoint = System.getProperty("tests.iceberg.s3.endpoint", "http://localhost:8333");
        final String accessKey = System.getProperty("tests.iceberg.s3.accessKey", "admin");
        final String secretKey = System.getProperty("tests.iceberg.s3.secretKey", "password");
        final String region = System.getProperty("tests.iceberg.s3.region", "us-east-1");

        helper = new IcebergTestHelper(restUri, s3Endpoint, accessKey, secretKey, region);
        helper.createNamespace(TEST_NAMESPACE);
        receivedRecords.clear();
    }

    @AfterEach
    void tearDown() {
        helper.dropTable(TEST_NAMESPACE, TEST_TABLE);
        helper.dropNamespace(TEST_NAMESPACE);
        helper.close();
    }

    @Test
    void export_writes_all_rows_to_buffer() throws Exception {
        // Create table and insert 3 rows
        final Table table = helper.createTable(TEST_NAMESPACE, TEST_TABLE, TEST_SCHEMA);
        final List<Record> rows = List.of(
                helper.newRecord(TEST_SCHEMA, 1, "Alice", 30),
                helper.newRecord(TEST_SCHEMA, 2, "Bob", 25),
                helper.newRecord(TEST_SCHEMA, 3, "Carol", 35)
        );
        helper.appendRows(table, rows);

        // Start IcebergSource and wait for events
        final IcebergService service = createService(false);
        final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer = createMockBuffer();
        service.start(buffer);

        try {
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(3))));

            // Verify all 3 rows are INSERT events
            assertThat(receivedRecords, hasSize(3));
            for (final org.opensearch.dataprepper.model.record.Record<Event> record : receivedRecords) {
                final Event event = record.getData();
                assertThat(event.getMetadata().getAttribute("iceberg_operation"), equalTo("INSERT"));
                assertThat(event.getMetadata().getAttribute("bulk_action"), equalTo("index"));
            }
        } finally {
            service.shutdown();
        }
    }

    @Test
    void cdc_insert_produces_insert_events() throws Exception {
        final CdcTestFixture fixture = createCdcTestFixture();

        try {
            // Insert new row (new snapshot)
            fixture.table.refresh();
            helper.appendRows(fixture.table, List.of(
                    helper.newRecord(TEST_SCHEMA, 4, "Dave", 40)
            ));

            // Wait for CDC event
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(fixture.afterInitialLoad + 1))));

            final Event lastEvent = receivedRecords.get(receivedRecords.size() - 1).getData();
            assertThat(lastEvent.getMetadata().getAttribute("iceberg_operation"), equalTo("INSERT"));
            assertThat(lastEvent.get("name", String.class), equalTo("Dave"));
        } finally {
            fixture.service.shutdown();
        }
    }

    @Test
    void cdc_update_removes_carryover_and_produces_correct_events() throws Exception {
        final CdcTestFixture fixture = createCdcTestFixture();

        try {
            // UPDATE: change Bob's age from 25 to 26 (CoW rewrites entire file)
            fixture.table.refresh();
            helper.overwriteRows(fixture.table, fixture.initialFile, List.of(
                    helper.newRecord(TEST_SCHEMA, 1, "Alice", 30),
                    helper.newRecord(TEST_SCHEMA, 2, "Bob", 26),   // changed
                    helper.newRecord(TEST_SCHEMA, 3, "Carol", 35)
            ));

            // Wait for CDC event: carryover for Alice and Carol removed,
            // UPDATE pair (DELETE old Bob + INSERT new Bob) merged into single INDEX
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(fixture.afterInitialLoad + 1))));

            final List<org.opensearch.dataprepper.model.record.Record<Event>> cdcEvents =
                    receivedRecords.subList(fixture.afterInitialLoad, receivedRecords.size());

            assertThat(cdcEvents, hasSize(1));

            final Event insertEvent = cdcEvents.get(0).getData();
            assertThat(insertEvent.getMetadata().getAttribute("iceberg_operation"), equalTo("INSERT"));
            assertThat(insertEvent.getMetadata().getAttribute("bulk_action"), equalTo("index"));
            assertThat(insertEvent.get("name", String.class), equalTo("Bob"));
            assertThat(insertEvent.get("age", Integer.class), equalTo(26));
        } finally {
            fixture.service.shutdown();
        }
    }

    @Test
    void cdc_delete_produces_delete_event() throws Exception {
        final CdcTestFixture fixture = createCdcTestFixture();

        try {
            // DELETE: remove Bob (CoW rewrites file without Bob)
            fixture.table.refresh();
            helper.overwriteRows(fixture.table, fixture.initialFile, List.of(
                    helper.newRecord(TEST_SCHEMA, 1, "Alice", 30),
                    helper.newRecord(TEST_SCHEMA, 3, "Carol", 35)
            ));

            // Wait for CDC event: should be DELETE(Bob) only
            // Carryover for Alice and Carol should be removed
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(fixture.afterInitialLoad + 1))));

            final List<org.opensearch.dataprepper.model.record.Record<Event>> cdcEvents =
                    receivedRecords.subList(fixture.afterInitialLoad, receivedRecords.size());

            assertThat(cdcEvents, hasSize(1));

            final Event deleteEvent = cdcEvents.get(0).getData();
            assertThat(deleteEvent.getMetadata().getAttribute("iceberg_operation"), equalTo("DELETE"));
            assertThat(deleteEvent.getMetadata().getAttribute("bulk_action"), equalTo("delete"));
            assertThat(deleteEvent.get("name", String.class), equalTo("Bob"));
        } finally {
            fixture.service.shutdown();
        }
    }

    /**
     * When a partition column is updated (e.g. region US -> EU), Iceberg produces
     * a DELETE in the old partition and an INSERT in the new partition. The shuffle
     * routes both to the same node by identifier_columns hash, enabling correct
     * UPDATE merge across partitions.
     */
    @Test
    void cdc_partition_column_update_correctly_handled_by_shuffle() throws Exception {
        final Schema partitionedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get())
        );

        final String partitionedTable = "partitioned_users";
        helper.dropTable(TEST_NAMESPACE, partitionedTable);
        final Table table = helper.createPartitionedTable(TEST_NAMESPACE, partitionedTable,
                partitionedSchema, org.apache.iceberg.PartitionSpec.builderFor(partitionedSchema).identity("region").build());

        // Insert initial data: id=1 in US, id=2 in EU
        final DataFile usFile = helper.appendRows(table, List.of(
                helper.newRecord(partitionedSchema, 1, "Alice", "US")
        ));
        final DataFile euFile = helper.appendRows(table, List.of(
                helper.newRecord(partitionedSchema, 2, "Bob", "EU")
        ));

        final String fullTableName = TEST_NAMESPACE + "." + partitionedTable;
        final IcebergService service = createServiceForTable(fullTableName, List.of("id"), false);
        final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer = createMockBuffer();
        service.start(buffer);

        try {
            // Wait for initial load (2 rows from 2 separate appends)
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(2))));
            final int afterInitialLoad = receivedRecords.size();

            // UPDATE: move id=1 from US to EU (partition column change)
            // Simulate CoW: delete old US file, add new EU file with id=1 in single overwrite
            table.refresh();
            final DataFile newEuFile = helper.writeDataFile(table, List.of(
                    helper.newRecord(partitionedSchema, 1, "Alice", "EU"),
                    helper.newRecord(partitionedSchema, 2, "Bob", "EU")
            ));
            table.newOverwrite()
                    .deleteFile(usFile)
                    .deleteFile(euFile)
                    .addFile(newEuFile)
                    .commit();

            // Wait for CDC events
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(afterInitialLoad + 1))));

            final List<org.opensearch.dataprepper.model.record.Record<Event>> cdcEvents =
                    receivedRecords.subList(afterInitialLoad, receivedRecords.size());

            // With shuffle: the DELETE(id=1, US) and INSERT(id=1, EU) are routed to the same
            // node by identifier_columns hash. UPDATE merge detects the pair and drops the DELETE.
            // Only the INSERT (INDEX action) should remain.
            boolean foundAliceEU = false;
            for (final org.opensearch.dataprepper.model.record.Record<Event> record : cdcEvents) {
                final Event event = record.getData();
                if ("Alice".equals(event.get("name", String.class))
                        && "EU".equals(event.get("region", String.class))) {
                    assertThat(event.getMetadata().getAttribute("bulk_action"), equalTo("index"));
                    foundAliceEU = true;
                }
            }
            assertThat("Expected INSERT event for Alice in EU after partition column update",
                    foundAliceEU, equalTo(true));

        } finally {
            service.shutdown();
            helper.dropTable(TEST_NAMESPACE, partitionedTable);
        }
    }

    /**
     * Common setup for CDC tests: creates a table with 3 rows (Alice, Bob, Carol),
     * starts IcebergService, and waits for initial load to complete.
     */
    private CdcTestFixture createCdcTestFixture() throws Exception {
        final Table table = helper.createTable(TEST_NAMESPACE, TEST_TABLE, TEST_SCHEMA);
        final DataFile initialFile = helper.appendRows(table, List.of(
                helper.newRecord(TEST_SCHEMA, 1, "Alice", 30),
                helper.newRecord(TEST_SCHEMA, 2, "Bob", 25),
                helper.newRecord(TEST_SCHEMA, 3, "Carol", 35)
        ));

        final IcebergService service = createService(false);
        final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer = createMockBuffer();
        service.start(buffer);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(receivedRecords, hasSize(greaterThanOrEqualTo(3))));

        return new CdcTestFixture(table, initialFile, service, receivedRecords.size());
    }

    private static class CdcTestFixture {
        final Table table;
        final DataFile initialFile;
        final IcebergService service;
        final int afterInitialLoad;

        CdcTestFixture(final Table table, final DataFile initialFile,
                       final IcebergService service, final int afterInitialLoad) {
            this.table = table;
            this.initialFile = initialFile;
            this.service = service;
            this.afterInitialLoad = afterInitialLoad;
        }
    }

    @SuppressWarnings("unchecked")
    private Buffer<org.opensearch.dataprepper.model.record.Record<Event>> createMockBuffer() throws Exception {
        final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer = mock(Buffer.class);
        doAnswer(invocation -> {
            final java.util.Collection<org.opensearch.dataprepper.model.record.Record<Event>> records =
                    invocation.getArgument(0);
            receivedRecords.addAll(records);
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());

        doAnswer(invocation -> {
            final org.opensearch.dataprepper.model.record.Record<Event> record = invocation.getArgument(0);
            receivedRecords.add(record);
            return null;
        }).when(buffer).write(any(), anyInt());

        return buffer;
    }

    private IcebergService createService(final boolean disableExport) throws Exception {
        return createServiceForTable(TEST_NAMESPACE + "." + TEST_TABLE, List.of("id"), disableExport);
    }

    private IcebergService createServiceForTable(final String fullTableName,
                                                  final List<String> identifierColumns,
                                                  final boolean disableExport) throws Exception {

        // Build config via reflection since fields are private
        final IcebergSourceConfig sourceConfig = mock(IcebergSourceConfig.class);
        final TableConfig tableConfig = mock(TableConfig.class);

        when(tableConfig.getTableName()).thenReturn(fullTableName);
        when(tableConfig.getCatalog()).thenReturn(helper.catalogProperties());
        when(tableConfig.getIdentifierColumns()).thenReturn(identifierColumns);
        when(tableConfig.isDisableExport()).thenReturn(disableExport);

        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));
        when(sourceConfig.getPollingInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);
        when(sourceConfig.getShuffleConfig()).thenReturn(createTestShuffleConfig());

        final EnhancedSourceCoordinator coordinator = createInMemoryCoordinator();
        coordinator.createPartition(new LeaderPartition());

        return new IcebergService(coordinator, sourceConfig, pluginMetrics, acknowledgementSetManager,
                org.opensearch.dataprepper.event.TestEventFactory.getTestEventFactory(), pluginFactory);
    }

    private org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleConfig createTestShuffleConfig() {
        try {
            final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue("{\"ssl\": false, \"port\": 4995}", 
                    org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleConfig.class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private EnhancedSourceCoordinator createInMemoryCoordinator() {
        final InMemorySourceCoordinationStore store = new InMemorySourceCoordinationStore(
                new org.opensearch.dataprepper.model.configuration.PluginSetting("in_memory", Collections.emptyMap()));
        final SourceCoordinationConfig coordinationConfig = new SourceCoordinationConfig(
                new PluginModel("in_memory", Collections.emptyMap()), null);
        final PluginMetrics coordinatorMetrics = PluginMetrics.fromNames("source-coordinator", "iceberg-it");
        final EnhancedLeaseBasedSourceCoordinator coordinator = new EnhancedLeaseBasedSourceCoordinator(
                store, coordinationConfig, coordinatorMetrics, "iceberg-it", new PartitionFactory());
        coordinator.initialize();
        return coordinator;
    }
}
