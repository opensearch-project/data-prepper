/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

class AbstractLogstashPluginAttributesMapperTest {

    private List<LogstashAttribute> logstashAttributes;
    private LogstashAttributesMappings mappings;
    private Map<String, Object> pluginSettings;

    @BeforeEach
    void setUp() {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);

        logstashAttributes = Collections.singletonList(logstashAttribute);
        mappings = mock(LogstashAttributesMappings.class);
        pluginSettings = new LinkedHashMap<>(mappings.getAdditionalAttributes());
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

}
