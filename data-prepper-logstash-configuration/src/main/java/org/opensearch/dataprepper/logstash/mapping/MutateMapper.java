/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class MutateMapper implements LogstashPluginAttributesMapper {
    public static class AddEntryConfig {
        public final String key;
        public final Object value;

        public AddEntryConfig(final String key, final Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class RenameCopyConfig {
        public final String from_key;
        public final String to_key;

        public RenameCopyConfig(final String fromKey, final String toKey) {
            this.from_key = fromKey;
            this.to_key = toKey;
        }
    }

    public static class SubstituteConfig {
        public final String source;
        public final String from;
        public final String to;

        public SubstituteConfig(final String source, final String from, final String to) {
            this.source = source;
            this.from = from;
            this.to = to;
        }
    }

    public List<PluginModel> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {
        final List<PluginModel> models = new LinkedList<>();
        final List<AddEntryConfig> adds = new LinkedList<>();
        final List<RenameCopyConfig> renames = new LinkedList<>();
        final List<String> deletes = new LinkedList<>();
        final List<RenameCopyConfig> copies = new LinkedList<>();
        final List<String> uppercases = new LinkedList<>();
        final List<String> lowercases = new LinkedList<>();
        final List<String> trims = new LinkedList<>();
        final List<SubstituteConfig> substitutes = new LinkedList<>();

        for(final LogstashAttribute attr : logstashAttributes) {
            final String name = attr.getAttributeName();
            if(Objects.equals(name, "add_field")) {
                ((Map<String, Object>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> adds.add(new AddEntryConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key), value)));
            } else if(Objects.equals(name, "rename")) {
                ((Map<String, String>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> renames.add(new RenameCopyConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key),
                                NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(value))));
            } else if(Objects.equals(name, "remove_field")) {
                deletes.addAll(((List<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
            } else if(Objects.equals(name, "copy")) {
                ((Map<String, String>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> copies.add(new RenameCopyConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key),
                                NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(value))));
            } else if(Objects.equals(name, "uppercase")) {
                uppercases.addAll(((ArrayList<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
            } else if(Objects.equals(name, "lowercase")) {
                lowercases.addAll(((ArrayList<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
            } else if(Objects.equals(name, "trim")) {
                trims.addAll(((ArrayList<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
            } else if(Objects.equals(name, "gsub")) {
                final List<String> array = ((ArrayList<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList());
                for(int i = 0; i < array.size(); i += 3) {
                    final SubstituteConfig newConfig = new SubstituteConfig(array.get(i), array.get(i + 1), array.get(i + 2));
                    substitutes.add(newConfig);
                }
            }
        }

        if(!renames.isEmpty()) {
            final Map<String, Object> renameMap = new HashMap<>();
            renameMap.put("entries", renames);

            final PluginModel renameModel = new PluginModel("rename_keys", renameMap);

            models.add(renameModel);
        }

        if(!copies.isEmpty()) {
            final Map<String, Object> copyMap = new HashMap<>();
            copyMap.put("entries", copies);

            final PluginModel renameModel = new PluginModel("copy_values", copyMap);

            models.add(renameModel);
        }

        if(!adds.isEmpty()) {
            final Map<String, Object> addMap = new HashMap<>();
            addMap.put("entries", adds);

            final PluginModel addModel = new PluginModel("add_entries", addMap);

            models.add(addModel);
        }

        if(!deletes.isEmpty()) {
            final Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("with_keys", deletes);

            final PluginModel deleteModel = new PluginModel("delete_entries", deleteMap);

            models.add(deleteModel);
        }

        if(!uppercases.isEmpty()) {
            final Map<String, Object> uppercaseMap = new HashMap<>();
            uppercaseMap.put("with_keys", uppercases);

            final PluginModel uppercaseModel = new PluginModel("uppercase_string", uppercaseMap);

            models.add(uppercaseModel);
        }

        if(!lowercases.isEmpty()) {
            final Map<String, Object> lowercaseMap = new HashMap<>();
            lowercaseMap.put("with_keys", lowercases);

            final PluginModel lowercaseModel = new PluginModel("lowercase_string", lowercaseMap);

            models.add(lowercaseModel);
        }

        if(!trims.isEmpty()) {
            final Map<String, Object> trimMap = new HashMap<>();
            trimMap.put("with_keys", trims);

            final PluginModel trimModel = new PluginModel("trim_string", trimMap);

            models.add(trimModel);
        }

        if(!substitutes.isEmpty()) {
            final Map<String, Object> substituteMap = new HashMap<>();
            substituteMap.put("entries", substitutes);

            final PluginModel substituteModel = new PluginModel("substitute_string", substituteMap);

            models.add(substituteModel);
        }

        return models;
    }
}