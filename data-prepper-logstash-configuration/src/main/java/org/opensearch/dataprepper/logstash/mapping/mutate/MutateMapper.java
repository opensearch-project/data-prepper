/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.mapping.LogstashAttributesMappings;
import org.opensearch.dataprepper.logstash.mapping.LogstashPluginAttributesMapper;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MutateMapper implements LogstashPluginAttributesMapper {
    private final Map<String, AbstractConversion> conversionMap = new HashMap<>();

    public MutateMapper() {
        conversionMap.put(RenameKeyConversion.getLogstashName(), new RenameKeyConversion());
        conversionMap.put(CopyValueConversion.getLogstashName(), new CopyValueConversion());
        conversionMap.put(AddEntryConversion.getLogstashName(), new AddEntryConversion());
        conversionMap.put(DeleteEntryConversion.getLogstashName(), new DeleteEntryConversion());
        conversionMap.put(SubstituteStringConversion.getLogstashName(), new SubstituteStringConversion());
        conversionMap.put(LowercaseStringConversion.getLogstashName(), new LowercaseStringConversion());
        conversionMap.put(UppercaseStringConversion.getLogstashName(), new UppercaseStringConversion());
        conversionMap.put(TrimStringConversion.getLogstashName(), new TrimStringConversion());
        conversionMap.put(SplitStringConversion.getLogstashName(), new SplitStringConversion());
    }

    public List<PluginModel> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {
        final List<AbstractConversion> converters = new LinkedList<>();
        for(final LogstashAttribute attr : logstashAttributes) {
            final String name = attr.getAttributeName();

            if(conversionMap.containsKey(name)) {
                final AbstractConversion converter = conversionMap.get(name);
                converter.addToModel(attr);

                if(!converters.contains(converter)) {
                    converters.add(converter);
                }
            }
        }

        final List<PluginModel> models = new LinkedList<>();
        for(final AbstractConversion converter : converters) {
            models.add(converter.generateModel());
        }

        return models;
    }
}