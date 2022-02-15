package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MutateMapper {
    public static class AddEntryConfig {
        public String key;
        public Object value;

        public AddEntryConfig(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class RenameConfig {
        public String from_key;
        public String to_key;

        public RenameConfig(String fromKey, String toKey) {
            this.from_key = fromKey;
            this.to_key = toKey;
        }
    }

    public List<PluginModel> getModels(LogstashPlugin logstashPlugin) {
        List<PluginModel> models = new LinkedList<>();
        List<AddEntryConfig> adds = new LinkedList<>();
        List<RenameConfig> renames = new LinkedList<>();

        for(LogstashAttribute attr : logstashPlugin.getAttributes()) {
            if(Objects.equals(attr.getAttributeName(), "add_field")) {
                adds.add(new AddEntryConfig(((LinkedHashMap)attr.getAttributeValue().getValue()).keySet().toArray()[0].toString(), ((LinkedHashMap)attr.getAttributeValue().getValue()).values().toArray()[0]));
            } else if(Objects.equals(attr.getAttributeName(), "rename")) {
                renames.add(new RenameConfig(((LinkedHashMap)attr.getAttributeValue().getValue()).keySet().toArray()[0].toString(), ((LinkedHashMap)attr.getAttributeValue().getValue()).values().toArray()[0].toString()));
            }
        }

        Map<String, Object> add_map = new HashMap<>();
        add_map.put("entries", adds);

        Map<String, Object> rename_map = new HashMap<>();
        rename_map.put("entries", renames);

        PluginModel addModel = new PluginModel("add_entries", add_map);
        PluginModel renameModel = new PluginModel("rename_keys", rename_map);

        if(adds.size() > 0) {
            models.add(addModel);
        }

        if(renames.size() > 0) {
            models.add(renameModel);
        }



        return models;
    }
}
