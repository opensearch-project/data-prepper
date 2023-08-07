package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ComposableIndexTemplate implements IndexTemplate {

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
