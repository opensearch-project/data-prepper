/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.OpenSearchPluginAttributesMapper.LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME;

class OpenSearchPluginAttributesMapperTest {

    private static final String DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE = "index";

    private OpenSearchPluginAttributesMapper createObjectUnderTest() {
        return new OpenSearchPluginAttributesMapper();
    }

    @Test
    void convert_missing_indexAttribute_to_return_empty_pluginSettings() {

        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());

        final List<PluginModel> actualPluginModel = createObjectUnderTest()
                .mapAttributes(Collections.emptyList(), logstashAttributesMappings);

        assertThat(actualPluginModel, Matchers.notNullValue());
        assertThat(actualPluginModel.size(), Matchers.equalTo(1));
        assertThat(actualPluginModel.get(0), Matchers.notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(0));
        assertThat(actualPluginModel.get(0).getPluginSettings(), not(hasKey(DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE)));
    }

    @Test
    void convert_emptyString_indexAttribute_to_return_pluginSettings_with_no_index_key() {

        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttribute.getAttributeName()).thenReturn(UUID.randomUUID().toString());
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(LogstashValueType.STRING);
        when(logstashAttributeValue.getValue()).thenReturn(UUID.randomUUID().toString());

        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());

        final List<PluginModel> actualPluginModel = createObjectUnderTest()
                .mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        assertThat(actualPluginModel, Matchers.notNullValue());
        assertThat(actualPluginModel.size(), Matchers.equalTo(1));
        assertThat(actualPluginModel.get(0), Matchers.notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(0));
        assertThat(actualPluginModel.get(0).getPluginSettings(), not(hasKey(DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE)));
    }



    @ParameterizedTest
    @ArgumentsSource(JodaToJava8IndicesArgumentsProvider.class)
    void convert_logstashIndexPattern_joda_to_dataPrepperIndexPattern_java8(final String logstashIndex, final String expectedIndex) {

        final LogstashAttribute logstashAttribute = createLogstashIndexAttribute(logstashIndex);

        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE));

        final List<PluginModel> actualPluginModel = createObjectUnderTest()
                .mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        assertThat(actualPluginModel, Matchers.notNullValue());
        assertThat(actualPluginModel.size(), Matchers.equalTo(1));
        assertThat(actualPluginModel.get(0), Matchers.notNullValue());

        assertThat(actualPluginModel.get(0).getPluginSettings(), notNullValue());
        assertThat(actualPluginModel.get(0).getPluginSettings().size(), equalTo(1));
        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE));
        assertThat(actualPluginModel.get(0).getPluginSettings().get(DATA_PREPPER_OPENSEARCH_INDEX_ATTRIBUTE), equalTo(expectedIndex));
    }

    static class JodaToJava8IndicesArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    Arguments.arguments("logstash-%{+yyyy.MM.dd}", "logstash-%{uuuu.MM.dd}"),
                    Arguments.arguments("logstash-index-%{+YYYY.MM.dd}", "logstash-index-%{yyyy.MM.dd}"),
                    Arguments.arguments("logstash-%{+xxxx.ww}", "logstash-%{YYYY.ww}"),
                    Arguments.arguments("my-index-name-%{+YYYY.MM.dd.HH}", "my-index-name-%{yyyy.MM.dd.HH}"),
                    Arguments.arguments("logstash", "logstash")
            );
        }
    }

    private LogstashAttribute createLogstashIndexAttribute(final String indexAttributeValue) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);

        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(LogstashValueType.STRING);
        when(logstashAttributeValue.getValue()).thenReturn(indexAttributeValue);

        return logstashAttribute;
    }
}
