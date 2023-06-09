package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    static class Wrapper {
        @JsonProperty("pipeline_extensions")
        @JsonSetter(nulls = Nulls.SKIP)
        private PipelineExtensions pipelineExtensions = new PipelineExtensions();

        @JsonCreator
        public Wrapper() {}
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Wrapper wrapper = new Wrapper();
        System.out.println(objectMapper.writeValueAsString(wrapper));
    }
}
