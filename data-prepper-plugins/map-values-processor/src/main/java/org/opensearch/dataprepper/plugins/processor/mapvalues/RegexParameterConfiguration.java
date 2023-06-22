package org.opensearch.dataprepper.plugins.processor.mapvalues;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class RegexParameterConfiguration {

    @JsonProperty("patterns")
    private HashMap<String, String> patterns;

    @JsonProperty("exact")
    private Boolean exact;

    public HashMap<String, String> getPatterns() { return patterns; }

    public Boolean getExact() { return exact; }

}
