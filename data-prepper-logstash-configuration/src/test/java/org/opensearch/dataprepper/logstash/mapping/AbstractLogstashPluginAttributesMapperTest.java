/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractLogstashPluginAttributesMapperTest {

    private List<LogstashAttribute> logstashAttributes;
    private LogstashAttributesMappings mappings;
    private Map<String, Object> pluginSettings;
    private Map<String, Object> defaultSettings;

    @BeforeEach
    void setUp() {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);

        logstashAttributes = Collections.singletonList(logstashAttribute);
        mappings = mock(LogstashAttributesMappings.class);
        pluginSettings = new LinkedHashMap<>(mappings.getAdditionalAttributes());
        defaultSettings = mappings.getDefaultSettings();
    }

    @Test
    void mapAttributes_with_no_custom_attributes_does_not_invoke_mapCustomAttributes_Test() {
        AbstractLogstashPluginAttributesMapper abstractLogstashPluginAttributesMapper = Mockito
                .spy(AbstractLogstashPluginAttributesMapper.class);

        when(abstractLogstashPluginAttributesMapper.getCustomMappedAttributeNames()).thenReturn(new HashSet<>());

        List<PluginModel> pluginModels = abstractLogstashPluginAttributesMapper.mapAttributes(logstashAttributes, mappings);
        verify(abstractLogstashPluginAttributesMapper, never()).mapCustomAttributes(
                logstashAttributes, mappings, pluginModels.get(0).getPluginSettings());
    }

    @Test
    void mapAttributes_with_custom_attributes_invokes_mapCustomAttributes_Test() {
        AbstractLogstashPluginAttributesMapper abstractLogstashPluginAttributesMapper = Mockito
                .spy(AbstractLogstashPluginAttributesMapper.class);

        when(abstractLogstashPluginAttributesMapper.getCustomMappedAttributeNames())
                .thenReturn(new HashSet<>(Collections.singletonList("customAttribute")));

        List<PluginModel> pluginModels = abstractLogstashPluginAttributesMapper.mapAttributes(logstashAttributes, mappings);
        verify(abstractLogstashPluginAttributesMapper).mapCustomAttributes(logstashAttributes, mappings, pluginModels.get(0).getPluginSettings());
    }

    @Test
    void mapAttributes_with_default_settings_Test() {
        AbstractLogstashPluginAttributesMapper abstractLogstashPluginAttributesMapper = Mockito
                .spy(AbstractLogstashPluginAttributesMapper.class);
        final String defaultSettingAttributeName = UUID.randomUUID().toString();
        final String defaultSettingAttributeValue = UUID.randomUUID().toString();
        when(mappings.getDefaultSettings()).thenReturn(Collections.singletonMap(defaultSettingAttributeName, defaultSettingAttributeValue));

        List<PluginModel> pluginModels = abstractLogstashPluginAttributesMapper.mapAttributes(Collections.emptyList(), mappings);

        assertThat(pluginModels, Matchers.notNullValue());
        assertThat(pluginModels.size(), Matchers.equalTo(1));
        assertThat(pluginModels.get(0), Matchers.notNullValue());
    }
}
