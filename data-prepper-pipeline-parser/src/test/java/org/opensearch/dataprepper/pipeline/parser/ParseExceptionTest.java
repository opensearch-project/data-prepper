/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ParseExceptionTest {

    private String message;
    private Throwable cause;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
        cause = Mockito.mock(Throwable.class);
    }

    @Test
    void getMessage_returns_the_message_provided_in_the_constructor() {
        final String message = UUID.randomUUID().toString();
        final ParseException objectUnderTest = new ParseException(message);

        assertThat(objectUnderTest.getMessage(), equalTo(message));
    }

    @Test
    void getCause_returns_the_cause_provided_in_the_constructor() {
        final Throwable cause = Mockito.mock(Throwable.class);
        final ParseException objectUnderTest = new ParseException(cause);

        assertThat(objectUnderTest.getCause(), equalTo(cause));
    }

    @Test
    void getMessage_returns_the_message_provided_in_the_two_parameter_constructor() {
        final ParseException objectUnderTest = new ParseException(message, cause);

        assertThat(objectUnderTest.getMessage(), equalTo(message));
    }

    @Test
    void getCause_returns_the_cause_provided_in_the_two_parameter_constructor() {
        final ParseException objectUnderTest = new ParseException(message, cause);

        assertThat(objectUnderTest.getCause(), equalTo(cause));
    }
}