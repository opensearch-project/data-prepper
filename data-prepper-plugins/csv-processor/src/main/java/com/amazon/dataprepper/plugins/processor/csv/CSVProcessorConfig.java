/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class CSVProcessorConfig {

    static final String DEFAULT_SOURCE = "message";
    static final char DEFAULT_DELIMITER = ',';
    static final char DEFAULT_QUOTE_CHARACTER = '\"'; // double quote

    static final boolean DEFAULT_DELETE_HEADERS = true;
    @JsonProperty("source")
    @NotNull
    private String source = DEFAULT_SOURCE;
    @JsonProperty("delimiter")
    @NotNull
    private char delimiter = DEFAULT_DELIMITER;
    @JsonProperty("delete_header")
    @NotNull
    private boolean deleteHeader = DEFAULT_DELETE_HEADERS;

    @JsonProperty("quote_character")
    @NotNull
    private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;
    @JsonProperty("column_names_source_key")
    private String columnNamesSourceKey;
    @JsonProperty("column_names")
    private String[] columnNames;

    public String getSource() {return source;}
    public char getDelimiter() {return delimiter;}
    public char getQuoteCharacter() {return quoteCharacter;}
    public boolean getDeleteHeader() {return deleteHeader;}
    public String getColumnNamesSourceKey() {return columnNamesSourceKey;}
    public String[] getColumnNames() { return columnNames;}

}
