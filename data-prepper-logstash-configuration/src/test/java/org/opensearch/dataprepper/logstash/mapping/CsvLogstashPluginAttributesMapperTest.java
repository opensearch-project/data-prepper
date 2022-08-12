/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.DATA_PREPPER_COLUMN_NAMES;
import static org.opensearch.dataprepper.logstash.mapping.CsvLogstashPluginAttributesMapper.LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME;

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
}