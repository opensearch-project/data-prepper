/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;


import com.amazon.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.KeyValueLogstashPluginAttributesMapper.LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.KeyValueLogstashPluginAttributesMapper.LOGSTASH_KV_TARGET_ATTRIBUTE_NAME;

@ExtendWith(MockitoExtension.class)
class KeyValueLogstashPluginAttributesMapperTest {
    KeyValueLogstashPluginAttributesMapper keyValueLogstashPluginAttributesMapper;

    @Mock
    private LogstashAttribute logstashAttribute;

    @Mock
    private LogstashAttributeValue logstashAttributeValue;

    @Mock
    private LogstashAttributesMappings logstashAttributesMappings;

    @BeforeEach
    void createObjectUnderTest() {
        keyValueLogstashPluginAttributesMapper = new KeyValueLogstashPluginAttributesMapper();
    }

    @ParameterizedTest
    @CsvSource({"[outer_key][inner_key],/outer_key/inner_key", "[array][0][key], /array/0/key", "message, message"})
    void convert_source_attribute_with_nested_syntax_will_return_converted_format(String input, String output) {
        when(logstashAttributeValue.getValue()).thenReturn(input);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);

        final String dataPrepperSourceAttribute = "source";
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME, dataPrepperSourceAttribute));

        final List<PluginModel> actualPluginModel =
                keyValueLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperSourceAttribute));
        assertThat(actualPluginModel.get(0).getPluginSettings().get(dataPrepperSourceAttribute), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().get(dataPrepperSourceAttribute), equalTo(output));
    }

    @ParameterizedTest
    @CsvSource({"[outer_key][inner_key],/outer_key/inner_key", "[array][0][key], /array/0/key", "message, message"})
    void convert_target_attribute_with_nested_syntax_will_return_converted_format(String input, String output) {
        when(logstashAttributeValue.getValue()).thenReturn(input);
        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_KV_TARGET_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);

        final String dataPrepperDestinationAttribute = "destination";
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_KV_TARGET_ATTRIBUTE_NAME, dataPrepperDestinationAttribute));

        final List<PluginModel> actualPluginModel =
                keyValueLogstashPluginAttributesMapper.mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(dataPrepperDestinationAttribute));
        assertThat(actualPluginModel.get(0).getPluginSettings().get(dataPrepperDestinationAttribute), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().get(dataPrepperDestinationAttribute), equalTo(output));
    }
}