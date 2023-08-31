package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipelineExtensions {

    @JsonAnySetter
    private final Map<String, Object> extensionMap;

    @JsonCreator
    public PipelineExtensions() {
        extensionMap = new HashMap<>();
    }

    @JsonAnyGetter
    public Map<String, Object> getExtensionMap() {
        return Collections.unmodifiableMap(extensionMap);
    }
}
