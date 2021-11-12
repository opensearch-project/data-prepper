/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultLogstashPluginAttributesMapperTest {
    private DefaultLogstashPluginAttributesMapper createObjectUnderTest() {
        return new DefaultLogstashPluginAttributesMapper();
    }

    @Test
    void mapAttributes_sets_mapped_attributes() {
        final String value = UUID.randomUUID().toString();
        final String logstashAttributeName = UUID.randomUUID().toString();
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttributeValue.getValue()).thenReturn(value);
        when(logstashAttribute.getAttributeName()).thenReturn(logstashAttributeName);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);

        final String dataPrepperAttribute = UUID.randomUUID().toString();
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(logstashAttributeName, dataPrepperAttribute));

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), mappings);

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(dataPrepperAttribute));
        assertThat(actualPluginSettings.get(dataPrepperAttribute), equalTo(value));
    }

    @Test
    void mapAttributes_sets_additional_attributes_to_those_values() {
        final String additionalAttributeName = UUID.randomUUID().toString();
        final String additionalAttributeValue = UUID.randomUUID().toString();
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getAdditionalAttributes()).thenReturn(Collections.singletonMap(additionalAttributeName, additionalAttributeValue));

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(Collections.emptyList(), mappings);

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(additionalAttributeName));
        assertThat(actualPluginSettings.get(additionalAttributeName), equalTo(additionalAttributeValue));
    }
}