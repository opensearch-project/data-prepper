/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtensionPluginConfigurationResolverTest {
    @Mock
    private ExtensionsConfiguration extensionsConfiguration;
    @Mock
    private PipelinesDataFlowModel pipelinesDataFlowModel;
    @Mock
    private PipelineExtensions pipelineExtensions;

    private ExtensionPluginConfigurationResolver objectUnderTest;

    @Test
    void testGetExtensionMap_defined_in_extensionsConfiguration_only() {
        when(extensionsConfiguration.getPipelineExtensions()).thenReturn(pipelineExtensions);
        final Map<String, Object> extensionMap = Map.of("test_extension", Map.of("test_key", "test_value"));
        when(pipelineExtensions.getExtensionMap()).thenReturn(extensionMap);
        when(pipelinesDataFlowModel.getPipelineExtensions()).thenReturn(null);
        objectUnderTest = new ExtensionPluginConfigurationResolver(extensionsConfiguration, pipelinesDataFlowModel);
        assertThat(objectUnderTest.getCombinedExtensionMap(), equalTo(extensionMap));
    }

    @Test
    void testGetExtensionMap_defined_in_pipelinesDataFlowModel_only() {
        when(extensionsConfiguration.getPipelineExtensions()).thenReturn(null);
        when(pipelinesDataFlowModel.getPipelineExtensions()).thenReturn(pipelineExtensions);
        final Map<String, Object> extensionMap = Map.of("test_extension", Map.of("test_key", "test_value"));
        when(pipelineExtensions.getExtensionMap()).thenReturn(extensionMap);
        objectUnderTest = new ExtensionPluginConfigurationResolver(extensionsConfiguration, pipelinesDataFlowModel);
        assertThat(objectUnderTest.getCombinedExtensionMap(), equalTo(extensionMap));
    }

    @Test
    void testGetExtensionMap_defined_in_both() {
        when(extensionsConfiguration.getPipelineExtensions()).thenReturn(pipelineExtensions);
        final Map<String, Object> dataPrepperConfigurationExtensionMap = Map.of(
                "test_extension1", Map.of("test_key1", "test_value1"),
                "test_extension2", Map.of("test_key1", "test_value1")
        );
        when(pipelineExtensions.getExtensionMap()).thenReturn(dataPrepperConfigurationExtensionMap);
        final PipelineExtensions pipelineExtensions1 = mock(PipelineExtensions.class);
        when(pipelinesDataFlowModel.getPipelineExtensions()).thenReturn(pipelineExtensions1);
        final Map<String, Object> pipelinesDataFlowModelExtensionMap = Map.of(
                "test_extension1", Map.of("test_key2", "test_value2")
        );
        when(pipelineExtensions1.getExtensionMap()).thenReturn(pipelinesDataFlowModelExtensionMap);
        objectUnderTest = new ExtensionPluginConfigurationResolver(extensionsConfiguration, pipelinesDataFlowModel);
        final Map<String, Object> expectedExtensionMap = Map.of(
                "test_extension1", Map.of("test_key2", "test_value2"),
                "test_extension2", Map.of("test_key1", "test_value1")
        );
        assertThat(objectUnderTest.getCombinedExtensionMap(), equalTo(expectedExtensionMap));
    }
}