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

    @JsonProperty("targets")
    @Valid
    private List<TargetsParameterConfig> targetsParameterConfigs = new ArrayList<>();

    public Object getSource() {
        return source;
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

    @AssertTrue(message = "The \"source\" field should either be a string or a list of strings sharing the same parent path.")
    public boolean isSourceFieldValid() {
        if(Objects.isNull(source)){
            return true;
        }
        if (source instanceof String) {
            return true;
        }
        if (source instanceof List<?>) {
            List<?> sourceList = (List<?>) source;
            if(sourceList.isEmpty()){
                return false;
            }
            return sourceList.stream().allMatch(sourceItem -> sourceItem instanceof String)
                   && commonRootPath(sourceList);
        }
        return false;
    }

    public boolean commonRootPath(List<?> sourceList){
        List<String> sources = (List<String>) sourceList;

        JsonExtractor jsonExtractor = new JsonExtractor();
        String firstSource = sources.get(0);
        String parentPath = jsonExtractor.getParentPath(firstSource);
        for (String source : sources) {
            if (!jsonExtractor.getParentPath(source).equals(parentPath)) {
                return false;
            }
        }
        return true;
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
