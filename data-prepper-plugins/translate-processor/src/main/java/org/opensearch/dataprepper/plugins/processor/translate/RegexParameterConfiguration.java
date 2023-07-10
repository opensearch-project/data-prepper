package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.Map;


public class RegexParameterConfiguration {

    final boolean DEFAULT_EXACT = true;
    @NotNull
    @JsonProperty("patterns")
    private Map<String, Object> patterns;

    @JsonProperty("exact")
    private Boolean exact = DEFAULT_EXACT;

    public Map<String, Object> getPatterns() {
        return patterns;
    }

    public Boolean getExact() { return exact; }

}
