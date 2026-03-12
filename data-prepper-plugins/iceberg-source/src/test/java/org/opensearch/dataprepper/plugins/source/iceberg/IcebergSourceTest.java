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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergSourceTest {

    @Mock
    private IcebergSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private TableConfig tableConfig;

    @Test
    void areAcknowledgementsEnabled_returnsConfigValue_whenTrue() {
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(true);
        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));

        final IcebergSource source = new IcebergSource(sourceConfig, pluginMetrics, acknowledgementSetManager);
        assertThat(source.areAcknowledgementsEnabled(), equalTo(true));
    }

    @Test
    void areAcknowledgementsEnabled_returnsConfigValue_whenFalse() {
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);
        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));

        final IcebergSource source = new IcebergSource(sourceConfig, pluginMetrics, acknowledgementSetManager);
        assertThat(source.areAcknowledgementsEnabled(), equalTo(false));
    }

    @Test
    void getPartitionFactory_returnsNonNull() {
        when(sourceConfig.getTables()).thenReturn(List.of(tableConfig));

        final IcebergSource source = new IcebergSource(sourceConfig, pluginMetrics, acknowledgementSetManager);
        assertThat(source.getPartitionFactory() != null, equalTo(true));
    }
}
