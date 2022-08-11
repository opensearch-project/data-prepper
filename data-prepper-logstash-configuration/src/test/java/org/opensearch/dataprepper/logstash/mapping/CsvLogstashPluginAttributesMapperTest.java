/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashConfigurationException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.DATA_PREPPER_COLUMN_NAMES;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_AUTODETECT_COLUMN_NAMES_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_AUTODETECT_COLUMN_NAMES_EXCEPTION_MESSAGE;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_CONVERT_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_CONVERT_EXCEPTION_MESSAGE;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_EMPTY_COLUMNS_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_EMPTY_COLUMNS_EXCEPTION_MESSAGE;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_EMPTY_ROWS_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_EMPTY_ROWS_EXCEPTION_MESSAGE;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_HEADER_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_SKIP_HEADER_EXCEPTION_MESSAGE;

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

        assertThat(pluginSettings.containsKey(DATA_PREPPER_COLUMN_NAMES), equalTo(true));
        assertThat(pluginSettings.containsValue(Collections.emptyList()), equalTo(true));
    }
    @Nested
    class InvalidConfigurationExceptions {
        final LogstashAttributesMappings mappings = mock(LogstashAttributesMappings.class);
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();

        @Test
        void when_autoDetectColumnNames_then_throws() {
            when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());
            final LogstashAttribute autoDetectColumnNames = LogstashAttribute.builder()
                    .attributeName(LOGSTASH_AUTODETECT_COLUMN_NAMES_ATTRIBUTE_NAME)
                    .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                    .build();

            Exception autoDetectColumnNamesException = assertThrows(LogstashConfigurationException.class, () ->
                    csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(autoDetectColumnNames),
                            mappings, pluginSettings)
            );

            assertThat(autoDetectColumnNamesException.getMessage(), equalTo(LOGSTASH_AUTODETECT_COLUMN_NAMES_EXCEPTION_MESSAGE));
        }

        @Test
        void when_skipEmptyRows_then_throws() {
            when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());
            final LogstashAttribute skipEmptyRows = LogstashAttribute.builder()
                    .attributeName(LOGSTASH_SKIP_EMPTY_ROWS_ATTRIBUTE_NAME)
                    .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                    .build();

            Exception skipEmptyRowsException = assertThrows(LogstashConfigurationException.class, () ->
                    csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(skipEmptyRows),
                            mappings, pluginSettings)
            );

            assertThat(skipEmptyRowsException.getMessage(), equalTo(LOGSTASH_SKIP_EMPTY_ROWS_EXCEPTION_MESSAGE));
        }

        @Test
        void when_skipEmptyColumns_then_throws() {
            when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());
            final LogstashAttribute skipEmptyColumns = LogstashAttribute.builder()
                    .attributeName(LOGSTASH_SKIP_EMPTY_COLUMNS_ATTRIBUTE_NAME)
                    .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                    .build();
            Exception skipEmptyColumnsException = assertThrows(LogstashConfigurationException.class, () ->
                    csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(skipEmptyColumns),
                            mappings, pluginSettings)
            );

            assertThat(skipEmptyColumnsException.getMessage(), equalTo(LOGSTASH_SKIP_EMPTY_COLUMNS_EXCEPTION_MESSAGE));
        }

        @Test
        void when_skipHeader_then_throws() {
            when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());
            final LogstashAttribute skipHeader = LogstashAttribute.builder()
                    .attributeName(LOGSTASH_SKIP_HEADER_ATTRIBUTE_NAME)
                    .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                    .build();

            Exception skipHeaderException = assertThrows(LogstashConfigurationException.class, () ->
                    csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(skipHeader),
                            mappings, pluginSettings)
            );

            assertThat(skipHeaderException.getMessage(), equalTo(LOGSTASH_SKIP_HEADER_EXCEPTION_MESSAGE));
        }

        @Test
        void when_convert_then_throws() {
            when(mappings.getMappedAttributeNames()).thenReturn(Collections.emptyMap());
            final LogstashAttribute convert = LogstashAttribute.builder()
                    .attributeName(LOGSTASH_CONVERT_ATTRIBUTE_NAME)
                    .attributeValue(LogstashAttributeValue.builder().attributeValueType(LogstashValueType.BAREWORD).value(true).build())
                    .build();

            Exception convertException = assertThrows(LogstashConfigurationException.class, () ->
                    csvLogstashPluginAttributesMapper.mapCustomAttributes(Collections.singletonList(convert),
                            mappings, pluginSettings)
            );

            assertThat(convertException.getMessage(), equalTo(LOGSTASH_CONVERT_EXCEPTION_MESSAGE));
        }
    }
}