/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.hamcrest.Matchers;
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
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME;
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

        final List<PluginModel> actualPluginModel =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), mappings);

        assertThat(actualPluginModel, Matchers.notNullValue());
        assertThat(actualPluginModel.size(), Matchers.equalTo(1));
        assertThat(actualPluginModel.get(0), Matchers.notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperAttribute));
        assertThat(actualPluginModel.get(0).getPluginSettings().get(dataPrepperAttribute), equalTo(value));

    }

    @Test
    void mapAttributes_sets_mapped_attributes_merging_multiple_match() {
        final LogstashAttribute matchMultiKeysLogstashAttribute = prepareHashTypeMatchLogstashAttribute(
                Arrays.asList(Map.entry("message", "fake message regex 1"), Map.entry("other", "fake other regex")));
        final LogstashAttribute matchMessageLogstashAttribute2 = prepareArrayTypeMatchLogstashAttribute("message", "fake message regex 2");
        final List<LogstashAttribute> matchLogstashAttributes = Arrays.asList(matchMultiKeysLogstashAttribute, matchMessageLogstashAttribute2);
        final Map<String, Object> expectedMatchSettings = Map.of("message", Arrays.asList("fake message regex 1", "fake message regex 2"),
                        "other", Collections.singletonList("fake other regex"));

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModels =
                createObjectUnderTest().mapAttributes(matchLogstashAttributes, mappings);

        assertThat(actualPluginModels, notNullValue());
        assertThat(actualPluginModels.size(), equalTo(1));
        assertThat(actualPluginModels.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginModels.get(0).getPluginSettings().get(dataPrepperMatchAttribute), equalTo(expectedMatchSettings));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapAttributes_sets_mapped_attributes_match_with_named_captures_and_pattern_definitions() {
        final String testNamedCapture = "test_named_capture";
        final String testRegex = "fake_regex";
        final String handWrittenPatternName = "handwritten_pattern_name";
        final String handWrittenPatternValue = "handwritten_pattern_value";
        final LogstashAttribute matchMessageLogstashAttributeWithNamedCaptures = prepareArrayTypeMatchLogstashAttribute(
                "message", String.format("(?<%s>%s)", testNamedCapture, testRegex));
        final LogstashAttribute patternDefinitionsLogstashAttribute = preparePatternDefinitionsLogstashAttribute(
                Map.of(handWrittenPatternName, handWrittenPatternValue)
        );

        final String dataPrepperMatchAttribute = "match";
        final String dataPrepperPatternDefinitionsAttribute = "pattern_definitions";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Map.of(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute,
                        LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME, dataPrepperPatternDefinitionsAttribute
                ));

        final List<PluginModel> actualPluginModels =
                createObjectUnderTest().mapAttributes(
                        Arrays.asList(matchMessageLogstashAttributeWithNamedCaptures, patternDefinitionsLogstashAttribute),
                        mappings);

        assertThat(actualPluginModels, notNullValue());
        assertThat(actualPluginModels.size(), equalTo(1));
        assertThat(actualPluginModels.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginModels.get(0).getPluginSettings(), hasKey(dataPrepperPatternDefinitionsAttribute));

        final Map<String, String> actualPatternDefinitions = (Map<String, String>) actualPluginModels.get(0).getPluginSettings().get(
                dataPrepperPatternDefinitionsAttribute);
        assertThat(actualPatternDefinitions, hasKey(handWrittenPatternName));
        assertThat(actualPatternDefinitions, hasValue(handWrittenPatternValue));
        assertThat(actualPatternDefinitions, hasValue(testRegex));
        final Map<String, List<String>> actualMatch = (Map<String, List<String>>) actualPluginModels.get(0).getPluginSettings().get(dataPrepperMatchAttribute);
        assertThat(actualMatch.get("message").get(0), matchesPattern(String.format("%%\\{(.*?):%s\\}", testNamedCapture)));
    }

    @Test
    void mapAttributes_converts_nested_syntax_in_mapped_attributes() {
        final LogstashAttribute matchMultiKeysLogstashAttribute = prepareHashTypeMatchLogstashAttribute(
                Arrays.asList(Map.entry("[text][message]", "fake message regex 1"), Map.entry("other", "fake other regex")));
        final LogstashAttribute matchMessageLogstashAttribute2 = prepareArrayTypeMatchLogstashAttribute("[text][message]", "%{NUMBER} %{GREEDYDATA:[nested][field][data2]}");
        final List<LogstashAttribute> matchLogstashAttributes = Arrays.asList(matchMultiKeysLogstashAttribute, matchMessageLogstashAttribute2);
        final Map<String, Object> expectedMatchSettings = Map.of("/text/message", Arrays.asList("fake message regex 1", "%{NUMBER} %{GREEDYDATA:/nested/field/data2}"),
                "other", Collections.singletonList("fake other regex"));

        final String dataPrepperMatchAttribute = "match";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, dataPrepperMatchAttribute));

        final List<PluginModel> actualPluginModels =
                createObjectUnderTest().mapAttributes(matchLogstashAttributes, mappings);

        assertThat(actualPluginModels, notNullValue());
        assertThat(actualPluginModels.size(), equalTo(1));
        assertThat(actualPluginModels.get(0).getPluginSettings(), hasKey(dataPrepperMatchAttribute));
        assertThat(actualPluginModels.get(0).getPluginSettings().get(dataPrepperMatchAttribute), equalTo(expectedMatchSettings));
    }

    @Test
    void mapAttributes_converts_nested_syntax_in_overwrite_mapped_attributes() {
        final LogstashAttribute matchMultiKeysLogstashAttribute = prepareOverwriteLogstashAttribute(Arrays.asList("[outer][key1]", "[outer][key2]"));
        final List<LogstashAttribute> grokAttributes = Collections.singletonList(matchMultiKeysLogstashAttribute);
        final List<String> expectedOverwriteKeys = Arrays.asList("/outer/key1", "/outer/key2");

        final String dataPrepperOverwriteAttribute = "keys_to_overwrite";
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap("overwrite", dataPrepperOverwriteAttribute));

        final List<PluginModel> actualPluginModels =
                createObjectUnderTest().mapAttributes(grokAttributes, mappings);

        assertThat(actualPluginModels, notNullValue());
        assertThat(actualPluginModels.size(), equalTo(1));
        assertThat(actualPluginModels.get(0).getPluginSettings(), hasKey(dataPrepperOverwriteAttribute));
        assertThat(actualPluginModels.get(0).getPluginSettings().get(dataPrepperOverwriteAttribute), equalTo(expectedOverwriteKeys));
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

    private LogstashAttribute preparePatternDefinitionsLogstashAttribute(final Map<String, String> patternDefinitions) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(HASH);
        when(logstashAttributeValue.getValue()).thenReturn(patternDefinitions);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        return logstashAttribute;
    }

    private LogstashAttribute prepareOverwriteLogstashAttribute(final List<String> keys) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(ARRAY);
        when(logstashAttributeValue.getValue()).thenReturn(keys);
        when(logstashAttribute.getAttributeName()).thenReturn("overwrite");
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        return logstashAttribute;
    }
}