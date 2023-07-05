/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


public class TranslateProcessorConfig {

    @JsonProperty("source")
    @NotNull
    private Object source;

    @JsonProperty("target")
    @NotNull
    @NotEmpty
    private String target;

    @JsonProperty("map")
    private Map<String, String> map;

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("default")
    private String defaultValue;

    @JsonProperty("translate_when")
    private String translateWhen;

    @JsonProperty("iterate_on")
    private String iterateOn;

    @JsonProperty("regex")
    private RegexParameterConfiguration regexParameterConfiguration;


    public Object getSource() { return source; }

    public String getTarget() { return target; }

    public Map<String, String> getMap() { return map; }

    public String getDefaultValue() { return defaultValue; }

    public String getFilePath() { return filePath; }

    public String getTranslateWhen() { return translateWhen; }

    public String getIterateOn() { return iterateOn; }

    public RegexParameterConfiguration getRegexParameterConfiguration(){ return regexParameterConfiguration; }


    @AssertTrue(message = "source field must be a string or list of strings")
    public boolean isSourceFieldValid(){
        if(source instanceof String){
            return true;
        }
        if(source instanceof List<?>){
            List<?>  sourceList = (List<?>) source;
            return sourceList.stream().allMatch(sourceItem -> sourceItem instanceof String);
        }
        return false;
    }

    @AssertTrue(message = "Either of map or patterns or file_path options need to be configured.")
    public boolean hasMappings() {
        return Stream.of(map, filePath, regexParameterConfiguration).filter(n -> n!=null).count() != 0;
    }

    @AssertTrue(message = "pattern option is mandatory while configuring regex option")
    public boolean isPatternPresent(){
        return regexParameterConfiguration == null || regexParameterConfiguration.getPatterns() != null;
    }

}
