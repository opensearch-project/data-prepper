/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

/**
 * Configuration class for {@link CsvProcessor}.
 */

@JsonPropertyOrder
@JsonClassDescription("The <code>csv</code> processor parses comma-separated values (CSVs) strings into structured data.")
public class CsvProcessorConfig {
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DELIMITER = ",";
    static final String DEFAULT_QUOTE_CHARACTER = "\""; // double quote
    static final Boolean DEFAULT_DELETE_HEADERS = true;

    @JsonProperty("source")
    @JsonPropertyDescription("The field in the event that will be parsed. Default value is <code>message</code>.")
    @NotBlank
    private String source = DEFAULT_SOURCE;

    @JsonProperty("delimiter")
    @JsonPropertyDescription("The character separating each column. Default value is <code>,</code>.")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("delete_header")
    @JsonPropertyDescription("If specified, the event header (<code>column_names_source_key</code>) is deleted after the event " +
            "is parsed. If there is no event header, no action is taken. Default value is true.")
    private Boolean deleteHeader = DEFAULT_DELETE_HEADERS;

    @JsonProperty("quote_character")
    @JsonPropertyDescription("The character used as a text qualifier for a single column of data. " +
            "Default value is <code>\"</code>.")
    private String quoteCharacter = DEFAULT_QUOTE_CHARACTER;

    @JsonProperty("column_names_source_key")
    @JsonPropertyDescription("The field in the event that specifies the CSV column names, which will be " +
            "automatically detected. If there are additional columns in the <code>source</code>, the column names are automatically " +
            "generated according to column index. If <code>column_names</code> is also defined, the header in " +
            "<code>column_names_source_key</code> can also be used to generate the event fields. " +
            "If too few columns are specified in this field, the remaining column names are automatically generated. " +
            "If too many column names are specified in this field, the CSV processor omits the extra column names.")
    private String columnNamesSourceKey;

    @JsonProperty("column_names")
    @JsonPropertyDescription("User-specified names for the CSV columns. " +
            "Default value is <code>[column1, column2, ..., columnN]</code> if there are no columns of data in the CSV " +
            "record and <code>column_names_source_key</code> is not defined. If <code>column_names_source_key</code> is defined, " +
            "the header in <code>column_names_source_key</code> generates the event fields. If too few columns are specified " +
            "in this field, the remaining column names are automatically generated. " +
            "If too many column names are specified in this field, the CSV processor omits the extra column names.")
    private List<String> columnNames;

    @JsonProperty("csv_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/some_key == \"test\"</code>. " +
            "If specified, the <code>csv</code> processor will only run on events when the expression evaluates to true. ")
    private String csvWhen;

    @JsonPropertyDescription("If true, the configured source field will be deleted after the CSV data is parsed into separate fields.")
    @JsonProperty
    private boolean deleteSource = false;

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

    public Boolean isDeleteSource() { return deleteSource; }

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

