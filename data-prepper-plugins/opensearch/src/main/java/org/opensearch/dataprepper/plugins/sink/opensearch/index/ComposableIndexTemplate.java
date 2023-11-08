package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ComposableIndexTemplate implements IndexTemplate {
    static final String TEMPLATE_KEY = "template";
    static final String INDEX_SETTINGS_KEY = "settings";

    private final Map<String, Object> indexTemplateMap;
    private String name;

    public ComposableIndexTemplate(final Map<String, Object> indexTemplateMap) {
        this.indexTemplateMap = new HashMap<>(indexTemplateMap);
    }

    @Override
    public void setTemplateName(final String name) {
        this.name = name;
    }

    @Override
    public void setIndexPatterns(final List<String> indexPatterns) {
        indexTemplateMap.put("index_patterns", indexPatterns);
    }

    @Override
    public void putCustomSetting(final String name, final Object value) {
        Map<String, Object> template = (Map<String, Object>) indexTemplateMap.computeIfAbsent(TEMPLATE_KEY, key -> new HashMap<>());

        Map<String, Object> settings = (Map<String, Object>) template.computeIfAbsent(INDEX_SETTINGS_KEY, key -> new HashMap<>());

        settings.put(name, value);
    }

    @Override
    public Optional<Long> getVersion() {
        if(!indexTemplateMap.containsKey("version"))
            return Optional.empty();
        final Number version = (Number) indexTemplateMap.get("version");
        return Optional.of(version.longValue());
    }

    public Map<String, Object> getIndexTemplateMap() {
        return Collections.unmodifiableMap(indexTemplateMap);
    }

    public String getName() {
        return name;
    }
}
