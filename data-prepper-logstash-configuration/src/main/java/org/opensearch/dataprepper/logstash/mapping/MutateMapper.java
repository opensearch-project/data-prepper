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

        public AddEntryConfig(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class RenameCopyConfig {
        public final String from_key;
        public final String to_key;

        public RenameCopyConfig(String fromKey, String toKey) {
            this.from_key = fromKey;
            this.to_key = toKey;
        }
    }

    public List<PluginModel> mapAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings) {
        List<PluginModel> models = new LinkedList<>();
        List<AddEntryConfig> adds = new LinkedList<>();
        List<RenameCopyConfig> renames = new LinkedList<>();
        List<String> deletes = new LinkedList<>();
        List<RenameCopyConfig> copies = new LinkedList<>();
        List<String> uppercases = new LinkedList<>();

        for(LogstashAttribute attr : logstashAttributes) {
            final String name = attr.getAttributeName();
            if(Objects.equals(name, "add_field")) {
                ((Map<String, Object>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> adds.add(new AddEntryConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key), value)));
            } else if(Objects.equals(name, "rename")) {
                ((Map<String, String>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> renames.add(new RenameCopyConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key),
                                NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(value))));
            } else if(Objects.equals(name, "remove_field")) {
<<<<<<< HEAD
                deletes.addAll((ArrayList<String>)attr.getAttributeValue().getValue());
            } else if(Objects.equals(name, "copy")) {
                ((Map<String, String>)attr.getAttributeValue().getValue()).entrySet().forEach(entry -> {
                    copies.add(new RenameCopyConfig(entry.getKey(), entry.getValue()));
                });
            } else if(Objects.equals(name, "uppercase")) {
                uppercases.addAll((ArrayList<String>)attr.getAttributeValue().getValue());
=======
                deletes.addAll(((List<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
            } else if(Objects.equals(name, "copy")) {
                ((Map<String, String>) attr.getAttributeValue().getValue()).forEach(
                        (key, value) -> copies.add(new RenameCopyConfig(NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key),
                                NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(value))));
            } else if(Objects.equals(name, "uppercase")) {
                uppercases.addAll(((ArrayList<String>) attr.getAttributeValue().getValue()).stream()
                        .map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList()));
>>>>>>> main
            }
        }

        if(!renames.isEmpty()) {
            Map<String, Object> renameMap = new HashMap<>();
            renameMap.put("entries", renames);

            PluginModel renameModel = new PluginModel("rename_keys", renameMap);

            models.add(renameModel);
        }

        if(!copies.isEmpty()) {
            Map<String, Object> copyMap = new HashMap<>();
            copyMap.put("entries", copies);

            PluginModel renameModel = new PluginModel("copy_values", copyMap);

            models.add(renameModel);
        }

        if(!adds.isEmpty()) {
            Map<String, Object> addMap = new HashMap<>();
            addMap.put("entries", adds);

            PluginModel addModel = new PluginModel("add_entries", addMap);

            models.add(addModel);
        }

<<<<<<< HEAD
        if(deletes.size() > 0) {
=======
        if(!deletes.isEmpty()) {
>>>>>>> main
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("with_keys", deletes);

            PluginModel deleteModel = new PluginModel("delete_entries", deleteMap);

            models.add(deleteModel);
        }

<<<<<<< HEAD
        if(uppercases.size() > 0) {
=======
        if(!uppercases.isEmpty()) {
>>>>>>> main
            Map<String, Object> uppercaseMap = new HashMap<>();
            uppercaseMap.put("with_keys", uppercases);

            PluginModel uppercaseModel = new PluginModel("uppercase_string", uppercaseMap);

            models.add(uppercaseModel);
        }

        return models;
    }
}
