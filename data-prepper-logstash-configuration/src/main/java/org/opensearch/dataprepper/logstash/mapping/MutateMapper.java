package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MutateMapper {
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

    public List<PluginModel> getModels(LogstashPlugin logstashPlugin) {
        List<PluginModel> models = new LinkedList<>();
        List<AddEntryConfig> adds = new LinkedList<>();
        List<RenameCopyConfig> renames = new LinkedList<>();
        List<ArrayList<String>> deletes = new LinkedList<>();
        List<RenameCopyConfig> copies = new LinkedList<>();

        for(LogstashAttribute attr : logstashPlugin.getAttributes()) {
            final String name = attr.getAttributeName();
            if(Objects.equals(name, "add_field")) {
                ((Map<String, Object>)attr.getAttributeValue().getValue()).entrySet().forEach(entry -> {
                    adds.add(new AddEntryConfig(entry.getKey(), entry.getValue()));
                });
            } else if(Objects.equals(name, "rename")) {
                ((Map<String, String>)attr.getAttributeValue().getValue()).entrySet().forEach(entry -> {
                    renames.add(new RenameCopyConfig(entry.getKey(), entry.getValue()));
                });
            } else if(Objects.equals(name, "remove_field")) {
                deletes.add((ArrayList<String>)attr.getAttributeValue().getValue());
            } else if(Objects.equals(name, "copy")) {
                ((Map<String, String>)attr.getAttributeValue().getValue()).entrySet().forEach(entry -> {
                    copies.add(new RenameCopyConfig(entry.getKey(), entry.getValue()));
                });
            }
        }

        if(adds.size() > 0) {
            Map<String, Object> add_map = new HashMap<>();
            add_map.put("entries", adds);

            PluginModel addModel = new PluginModel("add_entries", add_map);

            models.add(addModel);
        }

        if(renames.size() > 0) {
            Map<String, Object> rename_map = new HashMap<>();
            rename_map.put("entries", renames);

            PluginModel renameModel = new PluginModel("rename_keys", rename_map);

            models.add(renameModel);
        }

        if(deletes.size() > 0) {
            List<String> flatList = new LinkedList<>();
            for(ArrayList<String> list : deletes) {
                flatList.addAll(list);
            }

            Map<String, Object> delete_map = new HashMap<>();
            delete_map.put("with_keys", flatList);

            PluginModel deleteModel = new PluginModel("delete_entries", delete_map);

            models.add(deleteModel);
        }

        if(copies.size() > 0) {
            Map<String, Object> copy_map = new HashMap<>();
            copy_map.put("entries", copies);

            PluginModel renameModel = new PluginModel("copy_values", copy_map);

            models.add(renameModel);
        }

        return models;
    }
}
