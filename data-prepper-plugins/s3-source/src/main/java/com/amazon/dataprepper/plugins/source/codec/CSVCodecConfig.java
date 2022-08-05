/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;
import java.util.Objects;

public class CSVCodecConfig {
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

    public String getDelimiter() {
        return delimiter;
    }

    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    public List<String> getHeader() {
        return header;
    }

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

    @AssertTrue(message = "quote_character and delimiter cannot be the same character")
    boolean areDelimiterAndQuoteCharacterDifferent() {
        return !(delimiter.equals(quoteCharacter));
    }

    @AssertTrue(message = "header must not be an empty list. To autogenerate columns, set detect_header: false and delete header " +
            "from config.")
    boolean isValidHeader() {
        return Objects.isNull(header) || header.size() > 0;
    }
}