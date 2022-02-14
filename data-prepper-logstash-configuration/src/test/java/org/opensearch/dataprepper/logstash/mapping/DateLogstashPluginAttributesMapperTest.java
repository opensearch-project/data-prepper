/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.DateLogstashPluginAttributesMapper.LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.model.LogstashValueType.ARRAY;

class DateLogstashPluginAttributesMapperTest {
    private DateLogstashPluginAttributesMapper createObjectUnderTest() {
        return new DateLogstashPluginAttributesMapper();
    }

    @Test
    void convert_match_attribute_with_single_pattern_from_list_to_map_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList("logdate", "yyyy-MM-dd"));

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        final Map<String, List<String>> expectedMatchSettings = Map.of("logdate", Collections.singletonList("yyyy-MM-dd"));

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginSettings.get(dataPrepperMatchAttribute), equalTo(expectedMatchSettings));
    }

    @Test
    void convert_match_attribute_with_multiple_patterns_from_list_to_map_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList
                ("logdate", "yyyy-MM-dd", "yyyy-MM-dd hh:mm:ss")
        );

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        final Map<String, List<String>> expectedMatchSettings = Map.of("logdate", Arrays.asList("yyyy-MM-dd", "yyyy-MM-dd hh:mm:ss"));

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginSettings.get(dataPrepperMatchAttribute), equalTo(expectedMatchSettings));
    }

    @Test
    void convert_match_attribute_with_empty_match_from_list_to_empty_map_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Collections.emptyList()
        );

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final Map<String, Object> actualPluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        System.out.println(actualPluginSettings);

        final Map<String, List<String>> expectedMatchSettings = new HashMap<>();

        assertThat(actualPluginSettings, notNullValue());
        assertThat(actualPluginSettings.size(), equalTo(1));
        assertThat(actualPluginSettings, hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginSettings.get(dataPrepperMatchAttribute), equalTo(expectedMatchSettings));
    }

    private LogstashAttribute buildDateMatchLogstashAttribute(final List<String> match) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);

        List<String> value = new ArrayList<>(match);

        when(logstashAttributeValue.getAttributeValueType()).thenReturn(ARRAY);
        when(logstashAttributeValue.getValue()).thenReturn(value);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        return logstashAttribute;
    }
}