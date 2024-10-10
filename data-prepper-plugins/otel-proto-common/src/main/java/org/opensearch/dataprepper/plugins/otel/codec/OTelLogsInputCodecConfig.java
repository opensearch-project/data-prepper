/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
 
/**
 * Configuration class for {@link OTelLogsInputCodec}.
 */
public class OTelLogsInputCodecConfig {
    static final String JSON_FORMAT = "json"; 

    @JsonProperty("format")
    private String format = JSON_FORMAT;
 
    /**
     * The format of the OTel logs.
     * "json" by default.
     * @return The OTel logs format.
     */
    public String getFormat() {
         return format;
    }
 
    @AssertTrue(message = "format must be json.")
    boolean isValidFormat() {
        return JSON_FORMAT.equals(format);
    }

}