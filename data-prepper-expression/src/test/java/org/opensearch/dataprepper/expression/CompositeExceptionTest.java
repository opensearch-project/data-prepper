/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

class CompositeExceptionTest {

//    @Test
//    void testNoCauses() {
//        final Throwable throwable = new ParsingExceptions("test", Collections.emptyList());
//
//        final PrintStream printStream = mock(PrintStream.class);
//
//        throwable.printStackTrace(printStream);
//
//        verify(printStream, times(0)).println(eq("causes"));
//    }
//
//    @Test
//    void testMultipleExceptions() {
//        final Throwable cause = mock(Throwable.class);
//        final Throwable throwable = new ParsingExceptions("test", Collections.nCopies(5, cause));
//
//        final PrintStream printStream = mock(PrintStream.class);
//
//        throwable.printStackTrace(printStream);
//
//        verify(printStream, times(1)).println(eq("causes"));
//        verify(cause, times(5)).printStackTrace(eq(printStream));
//    }

    @Test
    void foo() throws CompositeException {
        throw new CompositeException(
                new RuntimeException("Error"),
                new RuntimeException("Error 2"),
                new RuntimeException("Error 3")
        );
    }

}