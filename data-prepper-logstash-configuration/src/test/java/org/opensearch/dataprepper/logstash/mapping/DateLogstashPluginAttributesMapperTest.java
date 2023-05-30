/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.processor.date.DateProcessorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashConfigurationException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.DateLogstashPluginAttributesMapper.LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.model.LogstashValueType.ARRAY;

class DateLogstashPluginAttributesMapperTest {
    DateLogstashPluginAttributesMapper dateLogstashPluginAttributesMapper;

    @BeforeEach
    void createObjectUnderTest() {
        dateLogstashPluginAttributesMapper = new DateLogstashPluginAttributesMapper();
    }

    @Test
    void convert_match_attribute_with_no_match_should_throw_LogstashConfigurationException() {
        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        Exception exception = assertThrows(LogstashConfigurationException.class, () ->
                dateLogstashPluginAttributesMapper.mapCustomAttributes(
                        Collections.emptyList(),
                        mappings,
                        Collections.emptyMap()));

        assertThat(exception.getMessage(), equalTo("Missing date match setting in Logstash configuration"));
    }

    @Test
    void convert_match_attribute_with_single_pattern_from_list_to_date_match_list_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList("logdate", "yyyy-MM-dd"));

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModel =
                dateLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<DateProcessorConfig.DateMatch> expectedMatchSettings =
                Collections.singletonList(new DateProcessorConfig.DateMatch("logdate", Collections.singletonList("yyyy-MM-dd")));
        final List<DateProcessorConfig.DateMatch> actualMatchSettings =
                (List<DateProcessorConfig.DateMatch>) actualPluginModel.get(0).getPluginSettings().get(dataPrepperMatchAttribute);

        assertThat(actualMatchSettings, notNullValue());
        assertThat(actualMatchSettings.size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualMatchSettings.get(0).getPatterns().size(), equalTo(1));
        assertThat(actualMatchSettings.get(0).getKey(), equalTo(expectedMatchSettings.get(0).getKey()));
        assertThat(actualMatchSettings.get(0).getPatterns(), equalTo(expectedMatchSettings.get(0).getPatterns()));
    }

    @Test
    void convert_match_attribute_with_multiple_patterns_from_list_to_date_match_list_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList
                ("logdate", "yyyy-MM-dd", "yyyy-MM-dd hh:mm:ss")
        );

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModel =
                dateLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<DateProcessorConfig.DateMatch> expectedMatchSettings =
                Collections.singletonList(new DateProcessorConfig.DateMatch("logdate", Arrays.asList("yyyy-MM-dd", "yyyy-MM-dd hh:mm:ss")));
        final List<DateProcessorConfig.DateMatch> actualMatchSettings =
                (List<DateProcessorConfig.DateMatch>) actualPluginModel.get(0).getPluginSettings().get(dataPrepperMatchAttribute);

        assertThat(actualMatchSettings, notNullValue());
        assertThat(actualMatchSettings.size(), equalTo(1));
        assertThat(actualMatchSettings.get(0).getPatterns().size(), equalTo(2));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualMatchSettings.get(0).getKey(), equalTo(expectedMatchSettings.get(0).getKey()));
        assertThat(actualMatchSettings.get(0).getPatterns(), equalTo(expectedMatchSettings.get(0).getPatterns()));
    }

    @Test
    void convert_match_attribute_with_empty_match_from_list_to_null_match_value_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Collections.emptyList());

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModel =
                dateLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<DateProcessorConfig.DateMatch> actualMatchSettings =
                (List<DateProcessorConfig.DateMatch>) actualPluginModel.get(0).getPluginSettings().get(dataPrepperMatchAttribute);

        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualMatchSettings.get(0), nullValue());
    }

    @Test
    void convert_match_attribute_with_nested_syntax_from_list_to_date_match_list_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList("[data][logdate]", "yyyy-MM-dd"));

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModel =
                dateLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(dateMatchAttribute), mappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<DateProcessorConfig.DateMatch> expectedMatchSettings =
                Collections.singletonList(new DateProcessorConfig.DateMatch("/data/logdate", Collections.singletonList("yyyy-MM-dd")));
        final List<DateProcessorConfig.DateMatch> actualMatchSettings =
                (List<DateProcessorConfig.DateMatch>) actualPluginModel.get(0).getPluginSettings().get(dataPrepperMatchAttribute);

        assertThat(actualMatchSettings, notNullValue());
        assertThat(actualMatchSettings.size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualMatchSettings.get(0).getPatterns().size(), equalTo(1));
        assertThat(actualMatchSettings.get(0).getKey(), equalTo(expectedMatchSettings.get(0).getKey()));
        assertThat(actualMatchSettings.get(0).getPatterns(), equalTo(expectedMatchSettings.get(0).getPatterns()));
    }

    @Test
    void convert_target_attribute_with_nested_syntax_from_list_to_date_match_list_test() {
        final LogstashAttribute dateMatchAttribute = buildDateMatchLogstashAttribute(Arrays.asList("[data][logdate]", "yyyy-MM-dd"));
        final String dataPrepperMatchAttribute = "match";
        final String dataPrepperDestinationAttribute = "destination";

        final LogstashAttribute targetLogstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);

        when(logstashAttributeValue.getValue()).thenReturn("[outer][inner]");
        when(targetLogstashAttribute.getAttributeName()).thenReturn("target");
        when(targetLogstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);

        Map<String, String> mappedAttributeNames = new HashMap<>();
        mappedAttributeNames.put(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute);
        mappedAttributeNames.put("target", dataPrepperDestinationAttribute);
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(mappedAttributeNames);
        when(mappings.getNestedSyntaxAttributeNames()).thenReturn(Collections.singletonList("target"));

        final List<PluginModel> actualPluginModel =
                dateLogstashPluginAttributesMapper.mapAttributes(Arrays.asList(targetLogstashAttribute, dateMatchAttribute), mappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<DateProcessorConfig.DateMatch> expectedMatchSettings =
                Collections.singletonList(new DateProcessorConfig.DateMatch("/data/logdate", Collections.singletonList("yyyy-MM-dd")));
        final List<DateProcessorConfig.DateMatch> actualMatchSettings =
                (List<DateProcessorConfig.DateMatch>) actualPluginModel.get(0).getPluginSettings().get(dataPrepperMatchAttribute);

        assertThat(actualMatchSettings, notNullValue());
        assertThat(actualMatchSettings.size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualMatchSettings.get(0).getPatterns().size(), equalTo(1));
        assertThat(actualMatchSettings.get(0).getKey(), equalTo(expectedMatchSettings.get(0).getKey()));
        assertThat(actualMatchSettings.get(0).getPatterns(), equalTo(expectedMatchSettings.get(0).getPatterns()));

        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey("destination"));
        final String expectedDestinationSettings = "/outer/inner";
        final String actualDestinationSettings = (String) actualPluginModel.get(0).getPluginSettings().get("destination");

        assertThat(actualDestinationSettings, notNullValue());
        assertThat(actualDestinationSettings, equalTo(expectedDestinationSettings));
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