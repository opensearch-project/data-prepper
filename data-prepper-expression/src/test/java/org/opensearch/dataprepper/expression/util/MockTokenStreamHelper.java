/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @since 1.3
 *
 * Helper class to generate mock {@link TokenStream} containing a list of {@link Token}
 */
public class MockTokenStreamHelper {
    private static final Logger LOG = LoggerFactory.getLogger(MockTokenStreamHelper.class);

    final List<Token> tokens = new ArrayList<>();
    Integer position = 0;

    /**
     * Initialize mock with list of tokens from types
     * @param types used to generate list of tokens
     */
    public MockTokenStreamHelper(final Integer ... types) {
        for (final Integer type : types) {
            addToken(type);
        }
    }

    /**
     * Add a token to the mock
     * @param type Token type to add
     */
    public void addToken(final Integer type) {
        final Token token = mock(Token.class);

        doAnswer(invocation -> {
            LOG.info("token.getType() -> {}", type);
            return type;
        }).when(token).getType();

        tokens.add(token);
    }

    public Object seek(final InvocationOnMock invocation) {
        final Integer prev = position;
        position = invocation.getArgument(0);
        LOG.trace("Seek {} -> {}", prev, position);
        return position;
    }

    /**
     * @since 1.3
     * Simulates {@link TokenStream#LT(int)}
     * @param invocation Context from {@link Mockito#doAnswer(Answer)}
     * @return current token from the queue
     */
    public Object LT(final InvocationOnMock invocation) {
        final Integer k = invocation.<Integer>getArgument(0);
        try {
            if (k > 1) {
                LOG.warn("Hello");
            }
            if (k >= 1) {
                return tokens.get(Math.min(position, tokens.size() -1));
            }
            else if (k == -1) {
                return tokens.get(position + k);
            }
            else {
                throw new RuntimeException("Unexpected K = " + k);
            }
        }
        catch (final IndexOutOfBoundsException e) {
            return null;
        }
    }

    /**
     * @since 1.3
     * Simulates {@link TokenStream#LA(int)}
     * @param invocation Context from {@link Mockito#doAnswer(Answer)}
     * @return current token type from the queue
     */
    public Object LA(final InvocationOnMock invocation) {
        final Object token = LT(invocation);
        if (token == null) {
            return null;
        }
        else {
            return ((Token) token).getType();
        }
    }
}
