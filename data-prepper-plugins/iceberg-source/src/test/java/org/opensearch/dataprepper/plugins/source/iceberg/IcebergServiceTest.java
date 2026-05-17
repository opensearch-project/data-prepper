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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergServiceTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private IcebergSourceConfig sourceConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private EventFactory eventFactory;
    @Mock
    private Buffer<Record<Event>> buffer;

    @Test
    @SuppressWarnings("unchecked")
    void start_uses_shared_catalog_when_table_catalog_is_null() {
        setupShuffleConfig();
        final Map<String, String> sharedCatalog = Map.of("type", "rest", "uri", "http://shared:8181");
        final TableConfig tableConfig = createTableConfig("db.my_table", null);

        when(sourceConfig.getCatalog()).thenReturn(sharedCatalog);
        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));
        when(sourceConfig.getPollingInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        final Catalog catalog = mock(Catalog.class);
        final Table table = mock(Table.class);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);

        final ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

        try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
            mockedCatalogUtil.when(() -> CatalogUtil.buildIcebergCatalog(anyString(), propsCaptor.capture(), isNull()))
                    .thenReturn(catalog);

            final IcebergService service = new IcebergService(sourceCoordinator, sourceConfig,
                    pluginMetrics, acknowledgementSetManager, eventFactory);
            service.start(buffer);
            service.shutdown();

            assertThat(propsCaptor.getValue(), equalTo(sharedCatalog));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void start_uses_table_catalog_when_table_catalog_is_set() {
        setupShuffleConfig();
        final Map<String, String> tableCatalog = Map.of("type", "glue", "warehouse", "s3://other/");
        final TableConfig tableConfig = createTableConfig("db.my_table", tableCatalog);

        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));
        when(sourceConfig.getPollingInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        final Catalog catalog = mock(Catalog.class);
        final Table table = mock(Table.class);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);

        final ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

        try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
            mockedCatalogUtil.when(() -> CatalogUtil.buildIcebergCatalog(anyString(), propsCaptor.capture(), isNull()))
                    .thenReturn(catalog);

            final IcebergService service = new IcebergService(sourceCoordinator, sourceConfig,
                    pluginMetrics, acknowledgementSetManager, eventFactory);
            service.start(buffer);
            service.shutdown();

            assertThat(propsCaptor.getValue(), equalTo(tableCatalog));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void start_uses_correct_catalog_for_each_table_in_mixed_config() {
        setupShuffleConfig();
        final Map<String, String> sharedCatalog = Map.of("type", "rest", "uri", "http://shared:8181");
        final Map<String, String> tableBCatalog = Map.of("type", "glue", "warehouse", "s3://other/");
        final TableConfig configA = createTableConfig("db.table_a", null);
        final TableConfig configB = createTableConfig("db.table_b", tableBCatalog);

        when(sourceConfig.getCatalog()).thenReturn(sharedCatalog);
        when(sourceConfig.getTables()).thenReturn(List.of(configA, configB));
        when(sourceConfig.getPollingInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        final Catalog catalog = mock(Catalog.class);
        final Table table = mock(Table.class);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(table);

        final ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

        try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
            mockedCatalogUtil.when(() -> CatalogUtil.buildIcebergCatalog(anyString(), propsCaptor.capture(), isNull()))
                    .thenReturn(catalog);

            final IcebergService service = new IcebergService(sourceCoordinator, sourceConfig,
                    pluginMetrics, acknowledgementSetManager, eventFactory);
            service.start(buffer);
            service.shutdown();

            final List<Map<String, String>> captured = propsCaptor.getAllValues();
            assertThat(captured.get(0), equalTo(sharedCatalog));
            assertThat(captured.get(1), equalTo(tableBCatalog));
        }
    }

    private TableConfig createTableConfig(final String tableName, final Map<String, String> catalogProps) {
        final TableConfig config = mock(TableConfig.class);
        when(config.getTableName()).thenReturn(tableName);
        when(config.getCatalog()).thenReturn(catalogProps);
        when(config.getIdentifierColumns()).thenReturn(Collections.emptyList());
        lenient().when(config.isDisableExport()).thenReturn(false);
        return config;
    }

    private void setupShuffleConfig() {
        final ShuffleConfig shuffleConfig = mock(ShuffleConfig.class);
        when(shuffleConfig.getServerPort()).thenReturn(4995);
        lenient().when(shuffleConfig.isSsl()).thenReturn(false);
        when(sourceConfig.getShuffleConfig()).thenReturn(shuffleConfig);
    }
}
