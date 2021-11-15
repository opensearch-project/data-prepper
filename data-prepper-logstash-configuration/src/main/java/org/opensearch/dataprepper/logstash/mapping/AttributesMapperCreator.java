/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;

import java.lang.reflect.Constructor;

class AttributesMapperCreator {

    LogstashPluginAttributesMapper createMapperClass(final String attributesMapperClassName) {
        final Class<?> attributesMapperClass;
        try {
            attributesMapperClass = Class.forName(attributesMapperClassName);
        } catch (final ClassNotFoundException ex) {
            throw new LogstashMappingException("Unable to find Mapper class with name of " + attributesMapperClassName, ex);
        }

        if(!LogstashPluginAttributesMapper.class.isAssignableFrom(attributesMapperClass)) {
            throw new LogstashMappingException("The provided mapping class does not implement " + LogstashPluginAttributesMapper.class);
        }

        try {
            final Constructor<?> defaultConstructor = attributesMapperClass.getDeclaredConstructor();
            final Object instance = defaultConstructor.newInstance();
            return  (LogstashPluginAttributesMapper) instance;
        } catch (final Exception ex) {
            throw new LogstashMappingException("Unable to create Mapper class with name of " + attributesMapperClassName, ex);
        }
    }
}
