/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.mapping.mutate.MutateMapper;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogstashPluginMapperTest {

    private PluginMapperProvider pluginMapperProvider;
    private LogstashPluginAttributesMapper logstashPluginAttributesMapper;

    @BeforeEach
    void setUp() {
        pluginMapperProvider = mock(PluginMapperProvider.class);

        logstashPluginAttributesMapper = mock(LogstashPluginAttributesMapper.class);
        when(logstashPluginAttributesMapper.mapAttributes(any(List.class), any(LogstashAttributesMappings.class)))
                .thenReturn(Collections.singletonList(new PluginModel("opensearch", null)));
        when(pluginMapperProvider.getAttributesMapper(any(LogstashMappingModel.class)))
                .thenReturn(logstashPluginAttributesMapper);
    }

    LogstashPluginMapper createObjectUnderTest() {
        return new LogstashPluginMapper(pluginMapperProvider);
    }

    @Test
    void mapPlugin_without_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.invalidMappingResourceNameData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        final LogstashPluginMapper objectUnderTest = createObjectUnderTest();
        Exception exception = assertThrows(LogstashMappingException.class, () ->
                objectUnderTest.mapPlugin(logstashPlugin));

        String expectedMessage = "Unable to find mapping resource " + mappingResourceName;
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_with_incorrect_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.invalidMappingResourceData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        final LogstashPluginMapper objectUnderTest = createObjectUnderTest();
        Exception exception = assertThrows(LogstashMappingException.class, () ->
                objectUnderTest.mapPlugin(logstashPlugin));

        String expectedMessage = "Unable to parse mapping file " + mappingResourceName;
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_with_custom_plugin_mapper_produces_plugins() {
        final LogstashPlugin logstashPlugin = mock(LogstashPlugin.class);
        when(logstashPlugin.getPluginName()).thenReturn("mutate");
        when(pluginMapperProvider.getAttributesMapper(any(LogstashMappingModel.class))).thenReturn(new MutateMapper());

        final LogstashPluginMapper objectUnderTest = createObjectUnderTest();
        final List<PluginModel> pluginModels = objectUnderTest.mapPlugin(logstashPlugin);

        assertThat(pluginModels, notNullValue());
    }

    @Test
    void mapPlugin_without_plugin_name_in_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.noPluginNameMappingResourceData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        final LogstashPluginMapper objectUnderTest = createObjectUnderTest();
        Exception exception = assertThrows(LogstashMappingException.class, () ->
                objectUnderTest.mapPlugin(logstashPlugin));

        String expectedMessage = "The mapping file " + mappingResourceName + " has a null value for 'pluginName'.";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_should_return_PluginModel_with_correct_pluginName() {
        final LogstashPlugin logstashPlugin = mock(LogstashPlugin.class);
        when(logstashPlugin.getPluginName()).thenReturn("amazon_es");

        final List<PluginModel> pluginModels = createObjectUnderTest().mapPlugin(logstashPlugin);

        assertThat(pluginModels, notNullValue());
        assertThat(pluginModels.get(0).getPluginName(), equalTo("opensearch"));
    }

    @Test
    void mapPlugin_should_return_PluginModel_with_mapped_attributes() {
        final LogstashPlugin logstashPlugin = mock(LogstashPlugin.class);
        when(logstashPlugin.getPluginName()).thenReturn("amazon_es");

        final Map<String, Object> mappedPluginSettings = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(logstashPluginAttributesMapper.mapAttributes(anyList(), any(LogstashAttributesMappings.class)))
                .thenReturn(Collections.singletonList(new PluginModel("opensearch", mappedPluginSettings)));

        final List<PluginModel> pluginModels = createObjectUnderTest().mapPlugin(logstashPlugin);

        assertThat(pluginModels, notNullValue());
        assertThat(pluginModels.get(0).getPluginSettings(), notNullValue());
        assertThat(pluginModels.get(0).getPluginSettings(), equalTo(mappedPluginSettings));
    }
}