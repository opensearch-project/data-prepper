/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
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


    private AbstractLogstashPluginAttributesMapper createObjectUnderTest() {
        return Mockito.spy(AbstractLogstashPluginAttributesMapper.class);
    }

    @Test
    void mapAttributes_with_no_custom_attributes_does_not_invoke_mapCustomAttributes_Test() {
        final AbstractLogstashPluginAttributesMapper objectUnderTest = createObjectUnderTest();

        when(objectUnderTest.getCustomMappedAttributeNames()).thenReturn(new HashSet<>());

        List<PluginModel> pluginModels = objectUnderTest.mapAttributes(logstashAttributes, mappings);
        verify(objectUnderTest, never()).mapCustomAttributes(
                logstashAttributes, mappings, pluginModels.get(0).getPluginSettings());
    }

    @Test
    void mapAttributes_with_custom_attributes_invokes_mapCustomAttributes_Test() {
        final AbstractLogstashPluginAttributesMapper objectUnderTest = createObjectUnderTest();

        when(objectUnderTest.getCustomMappedAttributeNames())
                .thenReturn(new HashSet<>(Collections.singletonList("customAttribute")));

        List<PluginModel> pluginModels = objectUnderTest.mapAttributes(logstashAttributes, mappings);
        verify(objectUnderTest).mapCustomAttributes(logstashAttributes, mappings, pluginModels.get(0).getPluginSettings());
    }

    @Test
    void mapAttributes_with_defaults_should_set_the_default_when_it_is_not_in_the_actual_attributes() {
        final AbstractLogstashPluginAttributesMapper objectUnderTest = createObjectUnderTest();
        final String defaultSettingAttributeName = UUID.randomUUID().toString();
        final String defaultSettingAttributeValue = UUID.randomUUID().toString();
        when(mappings.getDefaultSettings()).thenReturn(Collections.singletonMap(defaultSettingAttributeName, defaultSettingAttributeValue));
        when(mappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(defaultSettingAttributeName, defaultSettingAttributeName));

        List<PluginModel> pluginModels = objectUnderTest.mapAttributes(Collections.emptyList(), mappings);

        assertThat(pluginModels, Matchers.notNullValue());
        assertThat(pluginModels.size(), Matchers.equalTo(1));
        final PluginModel actualPluginModel = pluginModels.get(0);
        assertThat(actualPluginModel, Matchers.notNullValue());

        assertThat(actualPluginModel.getPluginSettings(), Matchers.notNullValue());
        assertThat(actualPluginModel.getPluginSettings(), hasKey(defaultSettingAttributeName));
        assertThat(actualPluginModel.getPluginSettings().get(defaultSettingAttributeName), equalTo(defaultSettingAttributeValue));
    }

    @Test
    void mapAttributes_with_defaults_should_use_the_actual_value_from_the_attribute_when_it_is_present() {
        final String defaultSettingName = UUID.randomUUID().toString();
        final String defaultSettingValue = UUID.randomUUID().toString();
        final String attributeValue = UUID.randomUUID().toString();

        when(mappings.getDefaultSettings()).thenReturn(Collections.singletonMap(defaultSettingName, defaultSettingValue));
        when(mappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(defaultSettingName, defaultSettingName));

        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        when(logstashAttribute.getAttributeName()).thenReturn(defaultSettingName);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        when(logstashAttributeValue.getValue()).thenReturn(attributeValue);

        final List<LogstashAttribute> logstashAttributes = Collections.singletonList(logstashAttribute);

        final List<PluginModel> pluginModels = createObjectUnderTest().mapAttributes(logstashAttributes, mappings);

        assertThat(pluginModels, notNullValue());
        assertThat(pluginModels.size(), equalTo(1));
        final PluginModel actualPluginModel = pluginModels.get(0);
        assertThat(actualPluginModel, notNullValue());

        assertThat(actualPluginModel.getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.getPluginSettings(), hasKey(defaultSettingName));
        assertThat(actualPluginModel.getPluginSettings().get(defaultSettingName), equalTo(attributeValue));
    }

    @Test
    void mapAttributes_with_defaults_should_set_the_default_attribute_name_when_different_from_logstash_attribute_name() {
        final String defaultSettingName = UUID.randomUUID().toString();
        final String defaultSettingValue = UUID.randomUUID().toString();
        final String attributeName = UUID.randomUUID().toString();
        final String attributeValue = UUID.randomUUID().toString();

        when(mappings.getDefaultSettings()).thenReturn(Collections.singletonMap(defaultSettingName, defaultSettingValue));
        when(mappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(attributeName, defaultSettingName));

        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        when(logstashAttribute.getAttributeName()).thenReturn(attributeName);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        when(logstashAttributeValue.getValue()).thenReturn(attributeValue);

        final List<LogstashAttribute> logstashAttributes = Collections.singletonList(logstashAttribute);

        final List<PluginModel> pluginModels = createObjectUnderTest().mapAttributes(logstashAttributes, mappings);

        assertThat(pluginModels, notNullValue());
        assertThat(pluginModels.size(), equalTo(1));
        final PluginModel actualPluginModel = pluginModels.get(0);
        assertThat(actualPluginModel, notNullValue());

        assertThat(actualPluginModel.getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.getPluginSettings(), hasKey(defaultSettingName));
        assertThat(actualPluginModel.getPluginSettings(), not(hasKey(attributeName)));
        assertThat(actualPluginModel.getPluginSettings().get(defaultSettingName), equalTo(attributeValue));
    }

}
