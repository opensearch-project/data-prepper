/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AttributesMapperCreatorTest {
    static class DoesNotImplement {
    }

    public static class NoDefaultConstructor implements LogstashPluginAttributesMapper {
        public NoDefaultConstructor(final String ignored) { }
        @Override
        public List<PluginModel> mapAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings) {
            return null;
        }
    }

    public static class ThrowingConstructor implements LogstashPluginAttributesMapper {
        public ThrowingConstructor() {
            throw new RuntimeException("Intentional exception for testing.");
        }
        @Override
        public List<PluginModel> mapAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings) {
            return null;
        }
    }

    public static class ValidMapper implements LogstashPluginAttributesMapper {
        @Override
        public List<PluginModel> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {
            return null;
        }
    }

    private AttributesMapperCreator createObjectUnderTest() {
        return new AttributesMapperCreator();
    }

    @Test
    void createMapperClass_should_throw_if_class_does_not_exist() {
        final AttributesMapperCreator objectUnderTest = createObjectUnderTest();

        assertThrows(LogstashMappingException.class,
                () -> objectUnderTest.createMapperClass("org.opensearch.dataprepper.logstash.DoesNotExist"));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoesNotImplement.class, NoDefaultConstructor.class, ThrowingConstructor.class
    })
    void createMapperClass_should_throw_for_classes_which_cannot_be_constructor(final Class<?> invalidClass) {
        final AttributesMapperCreator objectUnderTest = createObjectUnderTest();

        assertThrows(LogstashMappingException.class,
                () -> objectUnderTest.createMapperClass(invalidClass.getName()));
    }

    @Test
    void createMapperClass_returns_new_instance() {
        assertThat(createObjectUnderTest().createMapperClass(ValidMapper.class.getName()),
            instanceOf(LogstashPluginAttributesMapper.class));
    }
}