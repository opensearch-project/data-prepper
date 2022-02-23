/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

public class PluginMapperProvider {
    private final AttributesMapperCreator attributesMapperCreator;
    private final DefaultLogstashPluginAttributesMapper defaultLogstashPluginAttributesMapper;

    PluginMapperProvider() {
        this(new AttributesMapperCreator());
    }

    PluginMapperProvider(final AttributesMapperCreator attributesMapperCreator) {
        this.attributesMapperCreator = attributesMapperCreator;
        defaultLogstashPluginAttributesMapper = new DefaultLogstashPluginAttributesMapper();
    }

    LogstashPluginAttributesMapper getAttributesMapper(final LogstashMappingModel mappingModel) {
        final String attributesMapperClassName = mappingModel.getCustomPluginMapperClass() != null
                ? mappingModel.getCustomPluginMapperClass()
                : mappingModel.getAttributesMapperClass();

        if(attributesMapperClassName == null) {
            return defaultLogstashPluginAttributesMapper;
        }

        return attributesMapperCreator.createMapperClass(attributesMapperClassName);
    }
}
