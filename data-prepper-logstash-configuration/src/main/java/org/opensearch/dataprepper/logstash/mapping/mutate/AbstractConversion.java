/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.mapping.SubMutateAction;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractConversion<T> implements SubMutateAction {
    protected List<T> entries = new LinkedList<>();

    public void addToModel(final LogstashAttribute attribute) {
        if(attribute.getAttributeValue().getValue() instanceof Map) {
            ((Map<String, Object>) attribute.getAttributeValue().getValue())
                    .forEach(this::addKvToEntries);
        } else if(attribute.getAttributeValue().getValue() instanceof ArrayList) {
            addListToEntries((ArrayList<String>) attribute.getAttributeValue().getValue());
        }
    }

    public PluginModel generateModel() {
        final Map<String, Object> entryMap = new HashMap<>();
        entryMap.put(getMapKey(), entries);
        final PluginModel model = new PluginModel(getDataPrepperName(), entryMap);

        return model;
    }

    protected abstract void addKvToEntries(final String key, final Object value);

    protected abstract void addListToEntries(final ArrayList<String> list);

    protected abstract String getDataPrepperName();

    protected abstract String getMapKey();
}




