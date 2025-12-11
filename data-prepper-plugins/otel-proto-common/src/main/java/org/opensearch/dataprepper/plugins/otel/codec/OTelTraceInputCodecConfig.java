package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_trace</code> codec parses traces that follow the OpenTelemetry Protocol Specification. ")
public class OTelTraceInputCodecConfig {
    static final OTelFormatOption DEFAULT_FORMAT = OTelFormatOption.JSON;

    @JsonProperty(value = "format", defaultValue = "json")
    @JsonPropertyDescription("Specifies the format of the OTel trace.")
    @NotNull
    private OTelFormatOption format = DEFAULT_FORMAT;

    public OTelFormatOption getFormat() {
        return format;
    }
}
