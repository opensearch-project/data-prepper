package org.opensearch.dataprepper.plugins.processor.dissect;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Map;

@JsonPropertyOrder
@JsonClassDescription("The `dissect` processor extracts values from an event and maps them to individual fields " +
        "based on user-defined `dissect` patterns. The processor is well suited for field extraction from log " +
        "messages with a known structure.")
public class DissectProcessorConfig {
    @NotNull
    @JsonProperty("map")
    @JsonPropertyDescription("Defines the <code>dissect</code> patterns for specific keys. For details on how to define fields " +
            "in the <code>dissect</code> pattern, see (https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/dissect/#field-notations).")
    private Map<String, String> map;
    @JsonProperty("target_types")
    @JsonPropertyDescription("Specifies the data types for extract fields. Valid options are <code>integer</code>, " +
            "<code>double</code>, <code>string</code>, <code>long</code>, <code>big_decimal</code>, and <code>boolean</code>. By default, all fields are of the <code>string</code> type.")
    private Map<String, TargetType> targetTypes;
    @JsonProperty("dissect_when")
    @JsonPropertyDescription("Specifies a condition for performing the <code>dissect</code> operation using a " +
            "<a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">Data Prepper Expression Syntax</a>. " +
            "If specified, the <code>dissect</code> operation will only run when the expression evaluates to true.")
    private String dissectWhen;

    public String getDissectWhen(){
        return dissectWhen;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public Map<String, TargetType> getTargetTypes() { return targetTypes; }

}
