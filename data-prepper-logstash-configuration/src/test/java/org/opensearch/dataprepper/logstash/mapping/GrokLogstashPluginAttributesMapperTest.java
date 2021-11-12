/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.model.LogstashValueType.ARRAY;
import static org.opensearch.dataprepper.logstash.model.LogstashValueType.HASH;

class GrokLogstashPluginAttributesMapperTest {
    private GrokLogstashPluginAttributesMapper createObjectUnderTest() {
        return new GrokLogstashPluginAttributesMapper();
    }

    @Test
    void mapAttributes_sets_mapped_attributes_besides_match() {
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
    void mapAttributes_sets_mapped_attributes_merging_multiple_match() {
        final LogstashAttribute matchMultiKeysLogstashAttribute = prepareHashTypeMatchLogstashAttribute(
                Arrays.asList(Map.entry("message", "fake message regex 1"), Map.entry("other", "fake other regex")));
        final LogstashAttribute matchMessageLogstashAttribute2 = prepareArrayTypeMatchLogstashAttribute("message", "fake message regex 2");
        final List<LogstashAttribute> matchLogstashAttributes = Arrays.asList(matchMultiKeysLogstashAttribute, matchMessageLogstashAttribute2);
        final Map<String, Object> expectedPluginSettings = new HashMap<String, Object>() {{
            put("message", Arrays.asList("fake message regex 1", "fake message regex 2"));
            put("other", Collections.singletonList("fake other regex"));
        }};

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(new HashMap<String, String>() {{
            put(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute);
        }});

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(matchLogstashAttributes, mappings);

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginSettings.get(dataPrepperMatchAttribute), equalTo(expectedPluginSettings));
    }

    private LogstashAttribute prepareArrayTypeMatchLogstashAttribute(final String matchKey, final String matchValue) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        final List<String> value = Arrays.asList(matchKey, matchValue);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(ARRAY);
        when(logstashAttributeValue.getValue()).thenReturn(value);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        return logstashAttribute;
    }

    private LogstashAttribute prepareHashTypeMatchLogstashAttribute(final Collection<Map.Entry<String, String>> matchEntries) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        final Map<String, String> value = new HashMap<>();
        for (final Map.Entry<String, String> entry: matchEntries) {
            value.put(entry.getKey(), entry.getValue());
        }
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(HASH);
        when(logstashAttributeValue.getValue()).thenReturn(value);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        return logstashAttribute;
    }
}