/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.mapping.NestedSyntaxConverter;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractConversion<T> implements SubMutateAction {
    protected List<T> entries = new LinkedList<>();

    public void addToModel(final LogstashAttribute attribute) {
        if(attribute.getAttributeValue().getValue() instanceof Map) {
            ((Map<String, Object>) attribute.getAttributeValue().getValue())
                    .forEach((key, value) -> {
                        if(value instanceof String) {
                            addKvToEntries(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key),
                                    NestedSyntaxConverter.convertNestedSyntaxToJsonPointer((String) value));
                        } else {
                            addKvToEntries(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key), value);
                        }
                    });
        } else if(attribute.getAttributeValue().getValue() instanceof ArrayList) {
            addListToEntries(((ArrayList<String>) attribute.getAttributeValue().getValue()).stream()
                    .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
        }
    }

    public PluginModel generateModel() {
        final Map<String, Object> entryMap = new HashMap<>();
        entryMap.put(getMapKey(), entries);
        final PluginModel model = new PluginModel(getDataPrepperName(), entryMap);

        return model;
    }

    protected abstract void addKvToEntries(final String key, final Object value);

    protected abstract void addListToEntries(final List<String> list);

    protected abstract String getDataPrepperName();

    protected abstract String getMapKey();
}




