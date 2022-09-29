/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.DATA_PREPPER_COLUMN_NAMES;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_COLUMNS_ATTRIBUTE_NAME;

class CsvLogstashPluginAttributesMapperTest {
    CsvLogstashPluginAttributesMapper csvLogstashPluginAttributesMapper;

    @BeforeEach
    void createObjectUnderTest() {
        csvLogstashPluginAttributesMapper = new CsvLogstashPluginAttributesMapper();
    }

    @Test
    void when_autogenerateColumnsInLogstash_then_usesDataPrepperAutogenerateFunctionality() {
        final LogstashAttribute autoDetectColumnNames = LogstashAttribute.builder()
                .attributeName(LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME)
                .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                .build();
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());

        csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(autoDetectColumnNames),
                mappings, pluginSettings);

        assertThat(pluginSettings, hasKey(DATA_PREPPER_COLUMN_NAMES));
        assertThat(pluginSettings.get(DATA_PREPPER_COLUMN_NAMES), notNullValue());
        assertThat(((List<?>) pluginSettings.get(DATA_PREPPER_COLUMN_NAMES)).size(), equalTo(0));
    }

    @Test
    void when_autogenerateColumnsAndColumnsIsEmptyInLogstash_then_usesDataPrepperAutogenerateFunctionality() {
        final LogstashAttribute autoDetectColumnNames = LogstashAttribute.builder()
                .attributeName(LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME)
                .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                .build();

        final List<String> columns = new ArrayList<>();
        final LogstashAttribute columnsAttribute = LogstashAttribute.builder()
                .attributeName(LOGSTASH_COLUMNS_ATTRIBUTE_NAME)
                .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.ARRAY).value(columns).build())
                .build();

        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);

        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_COLUMNS_ATTRIBUTE_NAME, DATA_PREPPER_COLUMN_NAMES));

        final List<PluginModel> actualPluginModel = csvLogstashPluginAttributesMapper.mapAttributes(
                Collections.singletonList(columnsAttribute), mappings);

        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(autoDetectColumnNames),
                mappings, pluginSettings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<String> actualColumns = (List<String>) actualPluginModel.get(0).getPluginSettings().get(DATA_PREPPER_COLUMN_NAMES);
        final List<String> expectedColumns = columns;

        assertThat(actualColumns, notNullValue());
        assertThat(actualColumns.size(), equalTo(expectedColumns.size()));

        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(DATA_PREPPER_COLUMN_NAMES));
        assertThat(actualPluginModel.get(0).getPluginSettings().get(DATA_PREPPER_COLUMN_NAMES), notNullValue());
        assertThat(((List<?>) actualPluginModel.get(0).getPluginSettings().get(DATA_PREPPER_COLUMN_NAMES)).size(), equalTo(0));
    }

    @Test
    void when_autogenerateColumnsAndColumnsSetInLogstash_then_usesColumnsInsteadOfAutogenerating() {
        final LogstashAttribute autoDetectColumnNames = LogstashAttribute.builder()
                .attributeName(LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME)
                .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                .build();

        final List<String> columns = Arrays.asList("col1","col2");
        final LogstashAttribute columnsAttribute = LogstashAttribute.builder()
                .attributeName(LOGSTASH_COLUMNS_ATTRIBUTE_NAME)
                .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.ARRAY).value(columns).build())
                .build();

        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);

        when(mappings.getMappedAttributeNames()).thenReturn(
                Collections.singletonMap(LOGSTASH_COLUMNS_ATTRIBUTE_NAME, DATA_PREPPER_COLUMN_NAMES));

        final List<PluginModel> actualPluginModel = csvLogstashPluginAttributesMapper.mapAttributes(
                Collections.singletonList(columnsAttribute), mappings);

        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(autoDetectColumnNames),
                mappings, pluginSettings);

        assertThat(actualPluginModel, notNullValue());
        assertThat(actualPluginModel.size(), equalTo(1));
        assertThat(actualPluginModel.get(0), notNullValue());

        final List<String> actualColumns = (List<String>) actualPluginModel.get(0).getPluginSettings().get(DATA_PREPPER_COLUMN_NAMES);
        final List<String> expectedColumns = columns;

        assertThat(actualColumns, notNullValue());
        assertThat(actualColumns.size(), equalTo(expectedColumns.size()));

        assertThat(actualPluginModel.get(0).getPluginSettings(), hasKey(DATA_PREPPER_COLUMN_NAMES));
        assertThat(actualColumns.get(0), equalTo(expectedColumns.get(0)));
        assertThat(actualColumns.get(1), equalTo(expectedColumns.get(1)));
    }
}