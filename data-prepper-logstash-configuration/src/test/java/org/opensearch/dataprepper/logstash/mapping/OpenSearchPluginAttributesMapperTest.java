/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logstash.mapping.OpenSearchPluginAttributesMapper.LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.Map;

class OpenSearchPluginAttributesMapperTest {

    private OpenSearchPluginAttributesMapper createObjectUnderTest() {
        return new OpenSearchPluginAttributesMapper();
    }

    @Test
    void convert_logstash_index_date_time_pattern() {
        final String dataPrepperIndexAttribute = "index";
        final String value = "my-application-index";

        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);
        when(logstashAttributeValue.getValue()).thenReturn(value);
        when(logstashAttribute.getAttributeName()).thenReturn("index");
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);

        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, dataPrepperIndexAttribute));


        final Map<String, Object> pluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings.size(), equalTo(1));
        assertThat(pluginSettings, hasKey(dataPrepperIndexAttribute));
    }

    @Test
    void convert_logstashIndexPattern_joda_year_to_dataPrepperIndexPattern_java_year() {

        final LogstashAttribute logstashAttribute = createIndexAttributeValue("logstash-%{+yyyy.MM.dd}");

        final String dataPrepperIndexAttribute = "index";
        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, dataPrepperIndexAttribute));

        final Map<String, Object> pluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        final String expectedIndexPattern = "logstash-%{uuuu.MM.dd}";

        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings, hasKey(dataPrepperIndexAttribute));
        assertThat(pluginSettings.get(dataPrepperIndexAttribute), equalTo(expectedIndexPattern));
    }

    @Test
    void convert_logstashIndexPattern_joda_yearOfEra_to_dataPrepperIndexPattern_java_yearOfEra() {

        final LogstashAttribute logstashAttribute = createIndexAttributeValue("logstash-index-%{+YYYY.MM.dd}");

        final String dataPrepperIndexAttribute = "index";
        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, dataPrepperIndexAttribute));

        final Map<String, Object> pluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        final String expectedIndexPattern = "logstash-index-%{yyyy.MM.dd}";

        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings, hasKey(dataPrepperIndexAttribute));
        assertThat(pluginSettings.get(dataPrepperIndexAttribute), equalTo(expectedIndexPattern));
    }

    @Test
    void convert_logstashIndexPattern_joda_weekyear_to_dataPrepperIndexPattern_java_weekyear() {

        final LogstashAttribute logstashAttribute = createIndexAttributeValue("logstash-%{+xxxx.ww}");

        final String dataPrepperIndexAttribute = "index";
        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, dataPrepperIndexAttribute));

        final Map<String, Object> pluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        final String expectedIndexPattern = "logstash-%{YYYY.ww}";

        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings, hasKey(dataPrepperIndexAttribute));
        assertThat(pluginSettings.get(dataPrepperIndexAttribute), equalTo(expectedIndexPattern));
    }

    @Test
    void convert_logstashIndexPattern_time_to_dataPrepperIndexPattern_java_time() {

        final LogstashAttribute logstashAttribute = createIndexAttributeValue("my-index-name-%{+YYYY.MM.dd.HH}");

        final String dataPrepperIndexAttribute = "index";
        final LogstashAttributesMappings logstashAttributesMappings = mock(LogstashAttributesMappings.class);
        when(logstashAttributesMappings.getMappedAttributeNames()).thenReturn(Collections.singletonMap(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME, dataPrepperIndexAttribute));

        final Map<String, Object> pluginSettings =
                createObjectUnderTest().mapAttributes(Collections.singletonList(logstashAttribute), logstashAttributesMappings);

        final String expectedIndexPattern = "my-index-name-%{yyyy.MM.dd.HH}";

        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings, hasKey(dataPrepperIndexAttribute));
        assertThat(pluginSettings.get(dataPrepperIndexAttribute), equalTo(expectedIndexPattern));
    }


    private LogstashAttribute createIndexAttributeValue(final String index) {
        final LogstashAttribute logstashAttribute = mock(LogstashAttribute.class);
        final LogstashAttributeValue logstashAttributeValue = mock(LogstashAttributeValue.class);

        when(logstashAttribute.getAttributeName()).thenReturn(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME);
        when(logstashAttribute.getAttributeValue()).thenReturn(logstashAttributeValue);
        when(logstashAttributeValue.getAttributeValueType()).thenReturn(LogstashValueType.STRING);
        when(logstashAttributeValue.getValue()).thenReturn(index);

        return logstashAttribute;
    }
}
