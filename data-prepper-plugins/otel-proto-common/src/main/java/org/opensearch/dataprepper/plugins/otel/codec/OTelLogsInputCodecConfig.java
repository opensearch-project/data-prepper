/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonClassDescription; 

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_logs</code> codec parses log files in S3 that follow the OpenTelemetry Protocol Specification. " +
        "It creates a Data Prepper log event for each log record along with the resource attributes in the file.")
public class OTelLogsInputCodecConfig {
    static final OTelLogsFormatOption DEFAULT_FORMAT = OTelLogsFormatOption.JSON;
    
    @JsonProperty(value = "format", defaultValue = "json")
    @JsonPropertyDescription("Specifies the format of the OTel logs.")
    @NotNull
    private OTelLogsFormatOption format = DEFAULT_FORMAT;
 
    @JsonProperty(value = "length_prefixed_encoding", defaultValue = "false")
    @JsonPropertyDescription("Specifies if the length precedes the data in otlp_proto format")
    private boolean lengthPrefixedEncoding;

    @JsonProperty(value = "opensearch_mode", defaultValue = "true")
    @JsonPropertyDescription("Specifies if opensearch mode is enabled or not.")
    private boolean opensearchMode = true;

    public OTelLogsFormatOption getFormat() {
         return format;
    }

    @AssertTrue(message = "Not a valid format or JSON format used with length_prefixed_encoding")
    boolean isValidFormat() {
        if (format == null)
            return false;
        if (format == OTelLogsFormatOption.JSON && lengthPrefixedEncoding == true)
            return false;
        return true;
    }    

    public boolean getLengthPrefixedEncoding() {
        return lengthPrefixedEncoding;
    }

    public boolean getOpensearchMode() {
        return opensearchMode;
    }
}
