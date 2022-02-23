/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginMapperProviderTest {

    private AttributesMapperCreator attributesMapperCreator;
    private LogstashMappingModel logstashMappingModel;

    @BeforeEach
    void setUp() {
        attributesMapperCreator = mock(AttributesMapperCreator.class);
        logstashMappingModel = mock(LogstashMappingModel.class);
    }

    private PluginMapperProvider createObjectUnderTest() {
        return new PluginMapperProvider(attributesMapperCreator);
    }

    @Test
    void getAttributesMapper_should_return_Default_when_model_has_no_AttributesMapperClass() {
        final LogstashPluginAttributesMapper attributesMapper = createObjectUnderTest().getAttributesMapper(logstashMappingModel);

        assertThat(attributesMapper, notNullValue());
        assertThat(attributesMapper, instanceOf(DefaultLogstashPluginAttributesMapper.class));
    }

    @Test
    void getAttributesMapper_should_return_new_instance_for_AttributesMapperClass() {
        final String className = UUID.randomUUID().toString();
        when(logstashMappingModel.getAttributesMapperClass())
                .thenReturn(className);
        final LogstashPluginAttributesMapper expectedMapper = mock(LogstashPluginAttributesMapper.class);
        when(attributesMapperCreator.createMapperClass(className))
                .thenReturn(expectedMapper);

        assertThat(createObjectUnderTest().getAttributesMapper(logstashMappingModel), equalTo(expectedMapper));
    }
}