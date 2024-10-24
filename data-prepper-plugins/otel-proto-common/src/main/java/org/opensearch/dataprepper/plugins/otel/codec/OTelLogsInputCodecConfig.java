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
 
    public OTelLogsFormatOption getFormat() {
         return format;
    }

    @AssertTrue(message = "format must be json.")
    boolean isValidFormat() {
        return format != null;
    }    
}