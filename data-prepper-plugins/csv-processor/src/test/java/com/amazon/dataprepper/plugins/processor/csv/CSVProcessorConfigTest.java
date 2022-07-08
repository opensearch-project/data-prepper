/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import org.junit.Test;

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
        assertThat(objectUnderTest.getDeleteHeader(), equalTo(DEFAULT_DELETE_HEADERS));
        assertThat(objectUnderTest.getColumnNamesSourceKey(), equalTo(null));
        assertThat(objectUnderTest.getColumnNames(), equalTo(null));
    }
}
