package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_trace</code> codec parses log files in S3 that follow the OpenTelemetry Protocol Specification. " +
        "It creates a Data Prepper log event for each log record along with the resource attributes in the file.")
public class OTelTraceInputCodecConfig {
    static final OTelLogsFormatOption DEFAULT_FORMAT = OTelLogsFormatOption.JSON;

    @JsonProperty(value = "format", defaultValue = "json")
    @JsonPropertyDescription("Specifies the format of the OTel trace.")
    @NotNull
    private OTelLogsFormatOption format = DEFAULT_FORMAT;

    public OTelLogsFormatOption getFormat() {
        return format;
    }
}
