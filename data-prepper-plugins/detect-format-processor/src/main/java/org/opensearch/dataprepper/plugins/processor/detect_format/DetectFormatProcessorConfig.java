/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.detect_format;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The detect format processor detect format of the data in a specific field")
public class DetectFormatProcessorConfig {
    public static final String DEFAULT_KV_DELIMITER = "=";
    public static final List<String> DEFAULT_KV_SEPARATOR_LIST = List.of(" ", ",", "&");

    @JsonPropertyDescription("The source key on which to perform the operation.")
    @JsonProperty("source")
    @NotEmpty
    @NotNull
    private String source;

    @JsonPropertyDescription("Target key name where the detected format result is stored")
    @JsonProperty("target_key")
    private String targetKey;

    @JsonPropertyDescription("Target metadata key name where the detected format result is stored")
    @JsonProperty("target_metadata_key")
    private String targetMetadataKey;

    @JsonPropertyDescription("Key Value Delimiter")
    @JsonProperty("key_value_delimiter")
    private String kvDelimiter = DEFAULT_KV_DELIMITER;

    @JsonPropertyDescription("Key Value Separator List")
    @JsonProperty("key_value_separator_list")
    private List<String> kvSeparatorList = DEFAULT_KV_SEPARATOR_LIST;

    @JsonProperty("detect_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/some_key == \"test\"</code>. " +
            "If specified, the <code>detect_format</code> processor will only run on events when the expression evaluates to true. ")
    @ExampleValues({
        @Example(value = "/some_key == null", description = "Only runs parsing on the Event if some_key is null or doesn't exist.")
    })
    private String detectWhen;

    @AssertTrue(message = "At least one of target_key and target_metadata_key must be specified")
    boolean checkForTargetKeys() {
        return (targetKey != null || targetMetadataKey != null);
    }

    public String getSource() {
        return source;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public String getTargetMetadataKey() {
        return targetMetadataKey;
    }

    public String getDetectWhen() {
        return detectWhen;
    }

    public String getKVDelimiter() {
        return kvDelimiter;
    }

    public List<String> getKVSeparatorList() {
        return kvSeparatorList;
    }
}
