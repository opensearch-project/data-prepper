/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MockTokenStreamHelperTest {

    @Test
    void testConsumeWithQueuedTokens() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);
        helper.addToken(1);
        helper.addToken(2);
        helper.addToken(3);

        assertThat(helper.consume(null), is(nullValue()));
        assertThat(helper.consume(null), is(nullValue()));
        assertThat(helper.consume(null), is(nullValue()));
    }

    @Test
    void testConsumeWithNoQueuedTokens() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper();

        assertThat(helper.consume(null), is(nullValue()));
    }

    @Test
    void testLT() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);

        assertThat((Token) helper.LT(null), isA(Token.class));
        assertThat((Token) helper.LT(null), isA(Token.class));

        helper.consume(null);
        assertThrows(RuntimeException.class, () -> helper.LT(null));
    }

    @Test
    void testLA() {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(0);

        assertThat((Integer) helper.LA(null), isA(Integer.class));
        assertThat((Integer) helper.LA(null), isA(Integer.class));

        helper.consume(null);
        assertThrows(RuntimeException.class, () -> helper.LA(null));
    }
}