package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LegacyIndexTemplate implements IndexTemplate {

    public static final String SETTINGS_KEY = "settings";
    private final Map<String, Object> templateMap;

    public LegacyIndexTemplate(final Map<String, Object> templateMap) {
        this.templateMap = new HashMap<>(templateMap);
        if(this.templateMap.containsKey(SETTINGS_KEY)) {
            final HashMap<String, Object> copiedSettings = new HashMap<>((Map<String, Object>) this.templateMap.get(SETTINGS_KEY));
            this.templateMap.put(SETTINGS_KEY, copiedSettings);
        }
    }

    @Override
    public void setTemplateName(final String name) {
        templateMap.put("name", name);
    }

    @Override
    public void setIndexPatterns(final List<String> indexPatterns) {
        templateMap.put("index_patterns", indexPatterns);
    }

    @Override
    public void putCustomSetting(final String name, final Object value) {
        Map<String, Object> settings = (Map<String, Object>) this.templateMap.computeIfAbsent(SETTINGS_KEY, x -> new HashMap<>());
        settings.put(name, value);
    }

    @Override
    public Optional<Long> getVersion() {
        if(!templateMap.containsKey("version"))
            return Optional.empty();
        final Number version = (Number) templateMap.get("version");
        return Optional.of(version.longValue());
    }

    Map<String, Object> getTemplateMap() {
        return this.templateMap;
    }
}
