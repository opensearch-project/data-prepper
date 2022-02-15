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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @since 1.3
 *
 * Helper class to generate mock {@link TokenStream} containing a list of {@link Token}
 */
public class MockTokenStreamHelper {
    private static final Logger LOG = LoggerFactory.getLogger(MockTokenStreamHelper.class);

    final Queue<Token> tokenQueue = new LinkedBlockingQueue<>();

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

        doReturn("Type " + type)
                .when(token).toString();

        tokenQueue.add(token);
    }

    /**
     * Simulates consuming a token from the stream
     * @param invocation Context from {@link Mockito#doAnswer(Answer)}
     * @return null to match {@link TokenStream#consume()} method signature
     */
    public Object consume(final InvocationOnMock invocation) {
        if (!tokenQueue.isEmpty()) {
            final Token token = tokenQueue.remove();
            LOG.warn("Popped token {} -> {}, queue has {} items remaining", token, token.getType(), tokenQueue.size());
        }
        else {
            LOG.warn("Consumed empty queue!");
        }
        return null;
    }

    /**
     * @since 1.3
     * Simulates {@link TokenStream#LT(int)}
     * @param invocation Context from {@link Mockito#doAnswer(Answer)}
     * @return current token from the queue
     */
    public Object LT(final InvocationOnMock invocation) {
        if (tokenQueue.isEmpty()) {
            LOG.info("_input.LA() with empty queue");
            throw new RuntimeException("_input.LA() with empty queue");
        }
        else {
            final Token token = tokenQueue.peek();
            LOG.info("_input.LT() -> {}", token);
            return token;
        }
    }

    /**
     * @since 1.3
     * Simulates {@link TokenStream#LA(int)}
     * @param invocation Context from {@link Mockito#doAnswer(Answer)}
     * @return current token type from the queue
     */
    public Object LA(final InvocationOnMock invocation) {
        return ((Token) LT(invocation)).getType();
    }
}
