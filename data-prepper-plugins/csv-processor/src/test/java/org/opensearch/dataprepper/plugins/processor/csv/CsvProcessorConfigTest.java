/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.csv.CsvProcessorConfig.DEFAULT_DELETE_HEADERS;
import static org.opensearch.dataprepper.plugins.processor.csv.CsvProcessorConfig.DEFAULT_DELIMITER;
import static org.opensearch.dataprepper.plugins.processor.csv.CsvProcessorConfig.DEFAULT_QUOTE_CHARACTER;
import static org.opensearch.dataprepper.plugins.processor.csv.CsvProcessorConfig.DEFAULT_SOURCE;

public class CsvProcessorConfigTest {

    private CsvProcessorConfig createObjectUnderTest() {
        return new CsvProcessorConfig();
    }

    @Test
    public void test_when_defaultCsvProcessorConfig_then_returns_default_values() {
        final CsvProcessorConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSource(), equalTo(DEFAULT_SOURCE));
        assertThat(objectUnderTest.getDelimiter(), equalTo(DEFAULT_DELIMITER));
        assertThat(objectUnderTest.getQuoteCharacter(), equalTo(DEFAULT_QUOTE_CHARACTER));
        assertThat(objectUnderTest.isDeleteHeader(), equalTo(DEFAULT_DELETE_HEADERS));
        assertThat(objectUnderTest.getColumnNamesSourceKey(), equalTo(null));
        assertThat(objectUnderTest.getColumnNames(), equalTo(null));
        assertThat(objectUnderTest.isMultiLine(), equalTo(false));
    }

    @Nested
    class Validation {
        final CsvProcessorConfig csvProcessorConfig = createObjectUnderTest();

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

        @Test
        void isMultiLine_should_return_true_if_multi_line_is_set()
                throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(csvProcessorConfig, "multiLine", true);

            assertThat(csvProcessorConfig.isMultiLine(), equalTo(true));
        }
    }

    private void reflectivelySetField(final CsvProcessorConfig csvProcessorConfig, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = CsvProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(csvProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
