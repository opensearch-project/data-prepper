package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class RegexParameterConfiguration {

    @JsonProperty("patterns")
    private Map<String, String> patterns;

    @JsonProperty("exact")
    private Boolean exact;

    public Map<String, String> getPatterns() {
        return patterns;
    }

    public Boolean getExact() { return exact; }

}
