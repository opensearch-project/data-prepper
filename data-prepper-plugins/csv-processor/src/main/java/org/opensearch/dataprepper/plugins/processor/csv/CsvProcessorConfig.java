/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;

/**
 * Configuration class for {@link CsvProcessor}.
 */
public class CsvProcessorConfig {
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

    @JsonProperty("csv_when")
    private String csvWhen;

    /**
     * The field of the Event that contains the CSV data to be processed.
     *
     * @return The name of the source field.
     */
    public String getSource() {
        return source;
    }

    /**
     * The delimiter separating columns.
     *
     * @return The delimiter separating columns.
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Whether the field on the Event containing the header is deleted after the CSV data is parsed into separate fields.
     *
     * @return Whether column_names_source_key is deleted after parsing.
     */
    public Boolean isDeleteHeader() {
        return deleteHeader;
    }

    /**
     * The character used as a text qualifier for CSV data.
     *
     * @return The quote character.
     */
    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    /**
     * The field in the Event that specifies the CSV's column names, which will be autodetected and used for parsing.
     * If column_names is also defined, the header in `column_names_source_key` will be used to generate the Event fields.
     *
     * @return The name of the field containing the column names of the CSV data.
     */
    public String getColumnNamesSourceKey() {
        return columnNamesSourceKey;
    }

    /**
     * A list of user-specified column names for the CSV data.
     * If column_names_source_key is defined, the header in column_names_source_key will be used to generate the Event fields.
     *
     * @return The column names for the CSV data.
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getCsvWhen() { return csvWhen; }

    @AssertTrue(message = "delimiter must be exactly one character.")
    boolean isValidDelimiter() {
        return delimiter.length() == 1;
    }

    @AssertTrue(message = "quote_character must be exactly one character.")
    boolean isValidQuoteCharacter() {
        return quoteCharacter.length() == 1;
    }

    @AssertTrue(message = "quote_character and delimiter cannot be the same character")
    boolean areDelimiterAndQuoteCharacterDifferent() {
        return !(delimiter.equals(quoteCharacter));
    }
}

