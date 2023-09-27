/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DelimiterTest {

    private static final String DELIMITER_STRING = "test_string";
    private static final int START_END_INDEX = 3;
    private static final Delimiter DELIMITER = new Delimiter("test_delimiter");
    private Delimiter delimiter;

    @BeforeEach
    public void setUp() {
        delimiter = new Delimiter(DELIMITER_STRING);
    }

    @Test
    public void testSetAndGetStart() {
        delimiter.setStart(START_END_INDEX);
        assertThat(delimiter.getStart(), is(START_END_INDEX));
    }

    @Test
    public void testSetAndGetEnd() {
        delimiter.setEnd(START_END_INDEX);
        assertThat(delimiter.getEnd(), is(START_END_INDEX));
    }

    @Test
    public void testSetAndGetNext() {
        delimiter.setNext(DELIMITER);
        assertThat(delimiter.getNext(), is(DELIMITER));
    }

    @Test
    public void testSetAndGetPrev() {
        delimiter.setPrev(DELIMITER);
        assertThat(delimiter.getPrev(), is(DELIMITER));
    }
}