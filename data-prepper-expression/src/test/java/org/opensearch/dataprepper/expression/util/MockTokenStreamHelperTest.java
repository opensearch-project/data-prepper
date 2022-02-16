/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class MockTokenStreamHelperTest {

    @Test
    void testConsumeWithQueuedTokens() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);
        helper.addToken(1);

        final Invocation invocation = mock(Invocation.class);

        doAnswer(inv -> 1)
                .when(invocation)
                .getArgument(anyInt());

        assertThat(helper.seek(invocation), is(1));
        assertThat(helper.LA(invocation), is(1));
    }

    @Test
    void testLT() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);

        final Invocation invocation = mock(Invocation.class);
        doAnswer(inv -> 1)
                .when(invocation)
                .getArgument(anyInt());

        assertThat((Token) helper.LT(invocation), is(instanceOf(Token.class)));
        assertThat((Token) helper.LT(invocation), is(instanceOf(Token.class)));

        assertThat(helper.seek(invocation), is(1));

        helper.seek(invocation);
        assertThrows(RuntimeException.class, () -> helper.LT(null));
    }

    @Test
    void testLA() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);

        final Invocation invocation = mock(Invocation.class);
        doAnswer(inv -> 1)
                .when(invocation)
                .getArgument(anyInt());

        assertThat((Integer) helper.LA(invocation), is(instanceOf(Integer.class)));
        assertThat((Integer) helper.LA(invocation), is(instanceOf(Integer.class)));

        assertThat(helper.seek(invocation), is(1));

        helper.seek(invocation);
        assertThrows(RuntimeException.class, () -> helper.LA(null));
    }
}