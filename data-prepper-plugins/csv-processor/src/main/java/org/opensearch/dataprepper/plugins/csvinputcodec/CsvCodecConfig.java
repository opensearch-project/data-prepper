/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.csvinputcodec;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;
import java.util.Objects;

/**
 * Configuration class for {@link CsvInputCodec}.
 */
public class CsvCodecConfig {
    static final String DEFAULT_DELIMITER = ",";
    static final String DEFAULT_QUOTE_CHARACTER = "\""; // double quote
    static final Boolean DEFAULT_DETECT_HEADER = true;

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("quote_character")
    private String quoteCharacter = DEFAULT_QUOTE_CHARACTER;

    @JsonProperty("header")
    private List<String> header;

    @JsonProperty("detect_header")
    private Boolean detectHeader = DEFAULT_DETECT_HEADER;

    /**
     * The delimiter separating columns.
     * Comma "," by default.
     * @return The delimiter separating columns.
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * The character used as a text qualifier for CSV data.
     * Double quote "\"" by default.
     *
     * @return The quote character.
     */
    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    /**
     * The header containing the column names used to parse CSV data.
     *
     * @return The list of header/column names.
     */
    public List<String> getHeader() {
        return header;
    }

    /**
     * Whether the first line of the S3 Object should be interpreted as a header.
     * Defaults to true, meaning the first line of the S3 Object will be the header and will not be parsed like a regular CSV line.
     *
     * @return Whether the codec should detect the first line as a header.
     */
    public Boolean isDetectHeader() {
        return detectHeader;
    }

    @AssertTrue(message = "delimiter must be exactly one character.")
    boolean isValidDelimiter() {
        return delimiter.length() == 1;
    }

    @AssertTrue(message = "quote_character must be exactly one character.")
    boolean isValidQuoteCharacter() {
        return quoteCharacter.length() == 1;
    }

    @AssertTrue(message = "quote_character and delimiter cannot be the same character.")
    boolean areDelimiterAndQuoteCharacterDifferent() {
        return !(delimiter.equals(quoteCharacter));
    }

    @AssertTrue(message = "header must not be an empty list. To autogenerate columns, set detect_header: false and delete header " +
            "from config.")
    boolean isValidHeader() {
        return Objects.isNull(header) || header.size() > 0;
    }
}