package org.opensearch.dataprepper.plugins.processor.dissect;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

@JsonPropertyOrder
@JsonClassDescription("The <code>dissect</code> processor extracts values from an event and maps them to individual fields " +
        "based on user-defined <code>dissect</code> patterns. The processor is well suited for field extraction from log " +
        "messages with a known structure.")
public class DissectProcessorConfig {
    static final String DEFAULT_TARGET_TYPES = "string";

    @NotNull
    @JsonProperty("map")
    @JsonPropertyDescription("Defines the <code>dissect</code> patterns for specific keys. " +
            "Each key is a field name, and the value is the dissect pattern to use for dissecting it. " +
            "For details on how to define fields in the <code>dissect</code> pattern, see " + 
            "<a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/dissect/#field-notations\">here</a>. " +
            "An example dissect pattern is <code>%{Date} %{Time} %{Log_Type}: %{Message}</code>, which will dissect into four fields.")
    @ExampleValues({
        @Example(value = "%{Date} %{Time} %{Log_Type}: %{Message}", description = "Extracts the input data into the date, time, log_type, and message fields.")
    })
    private Map<String, String> map;

    @JsonProperty(value = "target_types", defaultValue = DEFAULT_TARGET_TYPES)
    @JsonPropertyDescription("Specifies the data types for extract fields. " +
            "Each key is a field name, and the value is the data type to use for that field. " +
            "Valid data types are <code>integer</code>, <code>double</code>, <code>string</code>, <code>long</code>, <code>big_decimal</code>, and <code>boolean</code>. " +
            "By default, all fields are treated as <code>string</code>.")
    private Map<String, String> targetTypes = Collections.emptyMap();;

    @JsonIgnore
    private Map<String, TargetType> targetTypeMap;

    @JsonProperty("dissect_when")
    @ExampleValues({
        @Example(value = "/log_type == \"ERROR\"", description = "Dissects when the log type is ERROR.")
    })
    @JsonPropertyDescription("Specifies a condition for performing the <code>dissect</code> operation using a " +
            "<a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>. " +
            "If specified, the <code>dissect</code> operation will only run when the expression evaluates to true. " +
            "For example, <code>/some_value == \"log\"</code>.")
    private String dissectWhen;

    public String getDissectWhen(){
        return dissectWhen;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public Map<String, TargetType> getTargetTypes() { return targetTypeMap; }

    @AssertTrue(message = "target_type must be a Map<String, TargetType>. Valid options include [ 'integer', 'string', 'double', 'boolean', 'long', and 'big_decimal' ]")
    boolean isTargetTypeValid() {
        try {
            targetTypeMap = targetTypes.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> TargetType.fromOptionValue(entry.getValue()))
                            );
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

}
