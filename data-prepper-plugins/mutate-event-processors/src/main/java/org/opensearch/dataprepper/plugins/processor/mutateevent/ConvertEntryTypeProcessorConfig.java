/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.typeconverter.BigDecimalConverter;

import java.util.List;
import java.util.Optional;

public class ConvertEntryTypeProcessorConfig  {
    @JsonProperty("key")
    private String key;

    @JsonProperty("keys")
    private List<String> keys;

    @JsonProperty("type")
    private TargetType type = TargetType.INTEGER;

    /**
     * Optional scale value used only in the case of BigDecimal converter
     */
    @JsonProperty("scale")
    private int scale = 0;

    @JsonProperty("convert_when")
    private String convertWhen;

    @JsonProperty("null_values")
    private List<String> nullValues;

    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure;

    public String getKey() {
        return key;
    }

    public List<String> getKeys() { return keys; }

    public TargetType getType() {
        if(type == TargetType.BIGDECIMAL && scale!=0) {
            ((BigDecimalConverter) type.getTargetConverter()).setScale(scale);
        }
        return type;
    }

    public String getConvertWhen() { return convertWhen; }

    public Optional<List<String>> getNullValues(){
        return Optional.ofNullable(nullValues);
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
