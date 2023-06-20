/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mapvalues;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;


public class MapValuesConfig {

    @JsonProperty("source")
    private String source;

    @JsonProperty("target")
    private String target;

    @JsonProperty("map")
    private Map<String, String> map;
    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("map_when")
    private String mapWhen;
    @JsonProperty("patterns")
    private HashMap<String, String> patterns;

    @JsonProperty("exact")
    private Boolean exact;
    @JsonProperty("regex")
    @JsonIgnore
    private Boolean regex;


    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public String getFilePath() { return filePath; }

    public String getMapWhen() {
        return mapWhen;
    }

    public HashMap<String, String> getPatterns() {
        return patterns;
    }

    public Boolean getExact() {
        return exact;
    }


}
