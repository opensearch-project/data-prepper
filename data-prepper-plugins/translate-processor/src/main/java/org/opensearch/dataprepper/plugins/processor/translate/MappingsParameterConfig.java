package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MappingsParameterConfig {

    @JsonProperty("source")
    @NotNull
    private Object source;

    @JsonProperty("iterate_on")
    private String iterateOn;

    @JsonProperty("targets")
    @Valid
    private List<TargetsParameterConfig> targetsParameterConfigs = new ArrayList<>();

    public Object getSource() {
        return source;
    }

    public String getIterateOn() {
        return iterateOn;
    }

    public List<TargetsParameterConfig> getTargetsParameterConfigs() {
        return targetsParameterConfigs;
    }

    @AssertTrue(message = "source option not configured")
    public boolean isSourcePresent(){
        return Objects.nonNull(source);
    }

    @AssertTrue(message = "targets option not configured")
    public boolean isTargetsPresent(){
        return Objects.nonNull(targetsParameterConfigs) && !targetsParameterConfigs.isEmpty();
    }

    @AssertTrue(message = "source field must be a string or list of strings")
    public boolean isSourceFieldValid() {
        if(Objects.isNull(source)){
            return true;
        }
        if (source instanceof String) {
            return true;
        }
        if (source instanceof List<?>) {
            List<?> sourceList = (List<?>) source;
            return sourceList.stream().allMatch(sourceItem -> sourceItem instanceof String);
        }
        return false;
    }

    public void parseMappings(){
        if(Objects.isNull(targetsParameterConfigs)){
            return;
        }
        for(TargetsParameterConfig targetsParameterConfig: targetsParameterConfigs){
            targetsParameterConfig.parseMappings();
        }
    }

    public void setTargetsParameterConfigs(List<TargetsParameterConfig> targetsParameterConfigs){
        this.targetsParameterConfigs = targetsParameterConfigs;
    }

}
