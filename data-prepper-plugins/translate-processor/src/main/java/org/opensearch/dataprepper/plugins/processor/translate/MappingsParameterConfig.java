package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class MappingsParameterConfig {

    @JsonProperty("source")
    @NotNull
    private Object source;

    @JsonProperty("iterate_on")
    private String iterateOn;

    @JsonProperty("targets")
    @NotNull
    private List<TargetsParameterConfig> targetsParameterConfigs;


    public Object getSource() {
        return source;
    }

    public String getIterateOn() {
        return iterateOn;
    }

    public List<TargetsParameterConfig> getTargetsParameterConfigs() {
        return targetsParameterConfigs;
    }

    public void parseMappings(){
        for(TargetsParameterConfig targetsParameterConfig: targetsParameterConfigs){
            targetsParameterConfig.parseMappings();
        }
    }

    @AssertTrue(message = "source field must be a string or list of strings")
    public boolean isSourceFieldValid() {
        if (source instanceof String) {
            return true;
        }
        if (source instanceof List<?>) {
            List<?> sourceList = (List<?>) source;
            return sourceList.stream().allMatch(sourceItem -> sourceItem instanceof String);
        }
        return false;
    }

}
