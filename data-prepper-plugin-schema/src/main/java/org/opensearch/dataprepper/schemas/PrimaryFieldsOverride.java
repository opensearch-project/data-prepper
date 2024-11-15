package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PrimaryFieldsOverride {

    @JsonAnySetter
    private final Map<String, Set<String>> primaryFieldsMap;

    @JsonCreator
    public PrimaryFieldsOverride() {
        primaryFieldsMap = new HashMap<>();
    }

    public Set<String> getPrimaryFieldsForComponent(final String componentName) {
        return primaryFieldsMap.getOrDefault(componentName, Collections.emptySet());
    }
}
