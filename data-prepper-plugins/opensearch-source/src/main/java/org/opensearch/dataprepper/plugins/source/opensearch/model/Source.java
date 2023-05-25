/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "type",
    "line_id",
    "play_name",
    "speech_number",
    "line_number",
    "speaker",
    "text_entry"
})

public class Source {

    @JsonProperty("type")
    private String type;

    @JsonProperty("line_id")
    private Integer lineId;

    @JsonProperty("play_name")
    private String playName;

    @JsonProperty("speech_number")
    private Integer speechNumber;

    @JsonProperty("line_number")
    private String lineNumber;

    @JsonProperty("speaker")
    private String speaker;

    @JsonProperty("text_entry")
    private String textEntry;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("line_id")
    public Integer getLineId() {
        return lineId;
    }

    @JsonProperty("line_id")
    public void setLineId(Integer lineId) {
        this.lineId = lineId;
    }

    @JsonProperty("play_name")
    public String getPlayName() {
        return playName;
    }

    @JsonProperty("play_name")
    public void setPlayName(String playName) {
        this.playName = playName;
    }

    @JsonProperty("speech_number")
    public Integer getSpeechNumber() {
        return speechNumber;
    }

    @JsonProperty("speech_number")
    public void setSpeechNumber(Integer speechNumber) {
        this.speechNumber = speechNumber;
    }

    @JsonProperty("line_number")
    public String getLineNumber() {
        return lineNumber;
    }

    @JsonProperty("line_number")
    public void setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
    }

    @JsonProperty("speaker")
    public String getSpeaker() {
        return speaker;
    }

    @JsonProperty("speaker")
    public void setSpeaker(String speaker) {
        this.speaker = speaker;
    }

    @JsonProperty("text_entry")
    public String getTextEntry() {
        return textEntry;
    }

    @JsonProperty("text_entry")
    public void setTextEntry(String textEntry) {
        this.textEntry = textEntry;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
