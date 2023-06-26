/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.stream.Stream;


public class TranslateProcessorConfig {

    @JsonProperty("source")
    @NotNull
    @NotEmpty
    private String source;

    @JsonProperty("target")
    @NotNull
    @NotEmpty
    private String target;

    @JsonProperty("map")
    private Map<String, String> map;
    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("map_when")
    private String mapWhen;

    @JsonProperty("regex")
    private RegexParameterConfiguration regexParameterConfiguration = new RegexParameterConfiguration();


    public String getSource() { return source; }

    public String getTarget() { return target; }

    public Map<String, String> getMap() { return map; }

    public String getFilePath() { return filePath; }

    public String getMapWhen() { return mapWhen; }

    public RegexParameterConfiguration getRegexParameterConfiguration(){ return regexParameterConfiguration; }


    @AssertTrue(message = "Either of map / patterns / file_path options need to be configured")
    public boolean hasAnyMappings() {
        return Stream.of(map, filePath, regexParameterConfiguration.getPatterns()).filter(n -> n!=null).count() != 0;
    }

}
