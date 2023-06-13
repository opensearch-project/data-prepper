/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CsvOutputCodecConfig {
    static final String DEFAULT_DELIMITER = ",";

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("header")
    private List<String> header;

    public String getDelimiter() {
        return delimiter;
    }

    public List<String> getHeader() {
        return header;
    }
}