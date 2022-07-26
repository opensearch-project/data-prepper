/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static com.amazon.dataprepper.plugins.processor.csv.CSVProcessorConfig.DEFAULT_SOURCE;
import static com.amazon.dataprepper.plugins.processor.csv.CSVProcessorConfig.DEFAULT_DELIMITER;
import static com.amazon.dataprepper.plugins.processor.csv.CSVProcessorConfig.DEFAULT_QUOTE_CHARACTER;
import static com.amazon.dataprepper.plugins.processor.csv.CSVProcessorConfig.DEFAULT_DELETE_HEADERS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CSVProcessorConfigTest {

    private CSVProcessorConfig createObjectUnderTest() {
        return new CSVProcessorConfig();
    }
    @Test
    public void test_when_defaultCSVProcessorConfig_then_returns_default_values() {
        CSVProcessorConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSource(), equalTo(DEFAULT_SOURCE));
        assertThat(objectUnderTest.getDelimiter(), equalTo(DEFAULT_DELIMITER));
        assertThat(objectUnderTest.getQuoteCharacter(), equalTo(DEFAULT_QUOTE_CHARACTER));
        assertThat(objectUnderTest.isDeleteHeader(), equalTo(DEFAULT_DELETE_HEADERS));
        assertThat(objectUnderTest.getColumnNamesSourceKey(), equalTo(null));
        assertThat(objectUnderTest.getColumnNames(), equalTo(null));
    }

    @Nested
    class Validation {
        final CSVProcessorConfig csvProcessorConfig = new CSVProcessorConfig();
        @BeforeEach
        void setUp() {}
        @Test
        void isValidDelimiter_should_return_false_if_delimiter_is_multiple_characters()
                throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(csvProcessorConfig, "delimiter", ";;;");

            assertThat(csvProcessorConfig.isValidDelimiter(), equalTo(false));
        }

        @Test
        void isValidDelimiter_should_return_true_if_delimiter_is_tab() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(csvProcessorConfig, "delimiter", "\t");

            assertThat(csvProcessorConfig.isValidDelimiter(), equalTo(true));
        }

        @Test
        void isValidQuoteCharacter_should_return_false_if_quote_char_is_multiple_characters()
                throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(csvProcessorConfig, "quoteCharacter", ";;;");

            assertThat(csvProcessorConfig.isValidQuoteCharacter(), equalTo(false));
        }

        @Test
        void isValidQuoteCharacter_should_return_true_if_quote_char_is_single_character()
                throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(csvProcessorConfig, "quoteCharacter", "\"");

            assertThat(csvProcessorConfig.isValidQuoteCharacter(), equalTo(true));
        }
    }

    private void reflectivelySetField(final CSVProcessorConfig csvProcessorConfig, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = CSVProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(csvProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
