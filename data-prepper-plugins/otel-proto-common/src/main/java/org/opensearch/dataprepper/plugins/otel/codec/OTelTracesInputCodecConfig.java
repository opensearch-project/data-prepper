/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The `otel_traces` codec parses trace files that follow the OpenTelemetry Protocol Specification. "
        + "It creates a Data Prepper Span event for each span record along with the resource attributes in the file.")
public class OTelTracesInputCodecConfig {

    static final OTelTracesFormatOption DEFAULT_FORMAT = OTelTracesFormatOption.JSON;
    static final OTelOutputFormat DEFAULT_OTEL_FORMAT = OTelOutputFormat.OPENSEARCH;

    @JsonProperty(value = "format", defaultValue = "json")
    @JsonPropertyDescription("Specifies the format of the OTel traces. Valid options are 'json' and 'protobuf'.")
    @NotNull
    private OTelTracesFormatOption format = DEFAULT_FORMAT;

    @JsonProperty(value = "otel_format", defaultValue = "opensearch")
    @JsonPropertyDescription("Specifies the output format of the decoded spans.")
    @NotNull
    private OTelOutputFormat otelFormat = DEFAULT_OTEL_FORMAT;

    @JsonProperty(value = "length_prefixed_encoding", defaultValue = "false")
    @JsonPropertyDescription("Specifies if the length precedes the data in protobuf format.")
    private boolean lengthPrefixedEncoding;

    public OTelTracesFormatOption getFormat() {
        return format;
    }

    public OTelOutputFormat getOTelOutputFormat() {
        return otelFormat;
    }

    @AssertTrue(message = "Not a valid format.")
    boolean isValidFormat() {
        return format != null;
    }

    public boolean getLengthPrefixedEncoding() {
        return lengthPrefixedEncoding;
    }
}
