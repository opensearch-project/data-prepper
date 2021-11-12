/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

class AttributesMapperProvider {
    private final AttributesMapperCreator attributesMapperCreator;
    private final DefaultLogstashPluginAttributesMapper defaultLogstashPluginAttributesMapper;

    AttributesMapperProvider() {
        this(new AttributesMapperCreator());
    }

    AttributesMapperProvider(final AttributesMapperCreator attributesMapperCreator) {
        this.attributesMapperCreator = attributesMapperCreator;
        defaultLogstashPluginAttributesMapper = new DefaultLogstashPluginAttributesMapper();
    }

    LogstashPluginAttributesMapper getAttributesMapper(final LogstashMappingModel mappingModel) {
        final String attributesMapperClassName = mappingModel.getAttributesMapperClass();
        if(attributesMapperClassName == null) {
            return defaultLogstashPluginAttributesMapper;
        }

        return attributesMapperCreator.createMapperClass(attributesMapperClassName);
    }
}
