/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import java.util.List;

public class CSVProcessorConfig {
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DELIMITER = ",";
    static final String DEFAULT_QUOTE_CHARACTER = "\""; // double quote
    static final Boolean DEFAULT_DELETE_HEADERS = true;

    @JsonProperty("source")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("delete_header")
    private Boolean deleteHeader = DEFAULT_DELETE_HEADERS;

    @JsonProperty("quote_character")
    private String quoteCharacter = DEFAULT_QUOTE_CHARACTER;

    @JsonProperty("column_names_source_key")
    private String columnNamesSourceKey;

    @JsonProperty("column_names")
    private List<String> columnNames;

    public String getSource() {
        return source;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Boolean isDeleteHeader() {
        return deleteHeader;
    }

    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    public String getColumnNamesSourceKey() {
        return columnNamesSourceKey;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @AssertTrue(message = "delimiter must be exactly one character.")
    boolean isValidDelimiter() {
        return delimiter.length() == 1;
    }

    @AssertTrue(message = "quote_character must be exactly one character.")
    boolean isValidQuoteCharacter() {
        return quoteCharacter.length() == 1;
    }
}
