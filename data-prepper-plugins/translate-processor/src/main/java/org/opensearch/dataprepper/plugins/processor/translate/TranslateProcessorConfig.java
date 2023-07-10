/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;
import org.opensearch.dataprepper.typeconverter.TypeConverter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private Map<String, Object> map;

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

    @JsonProperty("target_type")
    private TargetType targetType = TargetType.STRING;


    public Object getSource() { return source; }

    public String getTarget() { return target; }

    public Map<String, Object> getMap() { return map; }

    public String getDefaultValue() { return defaultValue; }

    public String getFilePath() { return filePath; }

    public String getTranslateWhen() { return translateWhen; }

    public String getIterateOn() { return iterateOn; }

    public TargetType getTargetType() { return targetType; }

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

    @AssertTrue(message = "The mapped values do not match the target type provided")
    public boolean isMapTypeValid() {
        return map.keySet().stream().allMatch(key -> checkTargetValueType(map.get(key)));
    }

    @AssertTrue(message = "The pattern values do not match the target type provided")
    public boolean isPatternTypeValid() {
        if (Objects.isNull(regexParameterConfiguration) || Objects.isNull(regexParameterConfiguration.getPatterns())) {
            return true;
        }
        Map<String, Object> patterns = regexParameterConfiguration.getPatterns();
        return patterns.keySet().stream().allMatch(key -> checkTargetValueType(patterns.get(key)));
    }

    private boolean checkTargetValueType(Object val) throws NumberFormatException {
        if (Objects.isNull(targetType)) {
            return true;
        }
        try {
            final TypeConverter converter = targetType.getTargetConverter();
            converter.convert(val);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }
}
