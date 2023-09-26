/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class DynamoDBSourceTest {

    private final String PLUGIN_NAME = "dynamo";

    private final String TEST_PIPELINE_NAME = "test_pipeline";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DynamoDBSourceConfig sourceConfig;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private SourceCoordinationStore coordinationStore;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginModel pluginModel;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;


    @Mock
    private Buffer<Record<Event>> buffer;

    private DynamoDBSource source;

    @BeforeEach
    void setup() {
        lenient().when(pluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        lenient().when(sourceConfig.getCoordinationStoreConfig()).thenReturn(pluginModel);
        lenient().when(sourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        lenient().when(pluginModel.getPluginName()).thenReturn(PLUGIN_NAME);
        lenient().when(pluginModel.getPluginSettings()).thenReturn(new HashMap<>());

        lenient().when(pluginFactory.loadPlugin(eq(SourceCoordinationStore.class), any(PluginSetting.class)))
                .thenReturn(coordinationStore);
        lenient().when(sourceConfig.getTableConfigs()).thenReturn(Collections.emptyList());
        lenient().doNothing().when(coordinationStore).initializeStore();
        lenient().when(coordinationStore.tryCreatePartitionItem(anyString(), anyString(), any(SourcePartitionStatus.class), anyLong(), anyString())).thenReturn(true);
        lenient().when(coordinationStore.tryCreatePartitionItem(anyString(), anyString(), any(SourcePartitionStatus.class), anyLong(), eq(null))).thenReturn(true);
        lenient().when(coordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class))).thenReturn(Optional.empty());
    }

    private DynamoDBSource createObjectUnderTest() {
        DynamoDBSource objectUnderTest = new DynamoDBSource(pluginMetrics, sourceConfig, pluginFactory, pluginSetting, awsCredentialsSupplier);
        return objectUnderTest;
    }

    @Test
    void test_create_source() {
        source = createObjectUnderTest();
        assertThat(source, notNullValue());
    }

}