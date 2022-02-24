/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ParsingExceptionsTest {

    @Test
    void testNoCauses() {
        final Throwable throwable = new ParsingExceptions("test", Collections.emptyList());

        final PrintStream printStream = mock(PrintStream.class);

        throwable.printStackTrace(printStream);

        verify(printStream, times(0)).println(eq("causes"));
    }

    @Test
    void testMultipleExceptions() {
        final Throwable cause = mock(Throwable.class);
        final Throwable throwable = new ParsingExceptions("test", Collections.nCopies(5, cause));

        final PrintStream printStream = mock(PrintStream.class);

        throwable.printStackTrace(printStream);

        verify(printStream, times(1)).println(eq("causes"));
        verify(cause, times(5)).printStackTrace(eq(printStream));
    }

}