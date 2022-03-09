/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ParseTreeCompositeExceptionTest {

    @Test
    void testNoCausesThrows() {
        assertThrows(NullPointerException.class, () -> new ParseTreeCompositeException(null));
    }

    @Test
    void testEmptyListThrows() {
        assertThrows(IllegalArgumentException.class, () -> new ParseTreeCompositeException(Collections.emptyList()));
    }

    @Test
    void testGivenSingleExceptionThenExceptionIsCause() {
        final RuntimeException mock = mock(RuntimeException.class);
        final ParseTreeCompositeException parseTreeCompositeException = new ParseTreeCompositeException(Arrays.asList(mock));

        assertThat(parseTreeCompositeException.getCause(), is(mock));
    }

    @Test
    void testMultipleExceptionsPrinted() throws ParseTreeCompositeException {
        final ParseTreeCompositeException parseTreeCompositeException = new ParseTreeCompositeException(Arrays.asList(
                new RuntimeException("Error"),
                new RuntimeException("Error 2"),
                new RuntimeException("Error 3"),
                null
        ));

        assertThat(parseTreeCompositeException.getCause() instanceof ExceptionOverview, is(true));

        final String message = parseTreeCompositeException.getCause().getMessage();
        assertThat(message, containsString("Multiple exceptions (4)"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error 3"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error 2"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error"));
        assertThat(message, containsString("|-- java.lang.NullPointerException: Throwable was null!"));
    }

    @Test
    void testExceptionWithoutStackTrace() {
        final Throwable cause = new Throwable();
        final ParseTreeCompositeException parseTreeCompositeException =
                new ParseTreeCompositeException(Collections.singletonList(cause));

        assertThat(parseTreeCompositeException.getCause() instanceof ExceptionOverview, is(true));
        final String message = parseTreeCompositeException.getCause().getMessage();
        //TODO: Fix test
        assertThat(message, is("Hello?"));
    }
}
