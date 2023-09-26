package org.opensearch.dataprepper.plugins.processor.dissect;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Map;

public class DissectProcessorConfig {
    @NotNull
    @JsonProperty("map")
    private Map<String, String> map;
    @JsonProperty("target_types")
    private Map<String, TargetType> targetTypes;
    @JsonProperty("dissect_when")
    private String dissectWhen;

    public String getDissectWhen(){
        return dissectWhen;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public Map<String, TargetType> getTargetTypes() { return targetTypes; }

}
