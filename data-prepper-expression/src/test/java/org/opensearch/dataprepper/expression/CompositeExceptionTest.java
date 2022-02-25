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

class CompositeExceptionTest {

    @Test
    void testNoCausesThrows() {
        assertThrows(NullPointerException.class, () -> new CompositeException(null));
    }

    @Test
    void testEmptyListThrows() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeException(Collections.emptyList()));
    }

    @Test
    void testGivenSingleExceptionThenExceptionIsCause() {
        final RuntimeException mock = mock(RuntimeException.class);
        final CompositeException compositeException = new CompositeException(Arrays.asList(mock));

        assertThat(compositeException.getCause(), is(mock));
    }

    @Test
    void foo() throws CompositeException {
        final CompositeException compositeException = new CompositeException(Arrays.asList(
                new RuntimeException("Error"),
                new RuntimeException("Error 2"),
                new RuntimeException("Error 3")
        ));

        assertThat(compositeException.getCause() instanceof ExceptionOverview, is(true));

        final String message = compositeException.getCause().getMessage();
        assertThat(message, containsString("Multiple exceptions (3)"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error 3"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error 2"));
        assertThat(message, containsString("|-- java.lang.RuntimeException: Error"));
    }
}
