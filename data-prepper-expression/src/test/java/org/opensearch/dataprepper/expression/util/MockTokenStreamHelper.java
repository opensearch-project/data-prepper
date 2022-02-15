/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.Token;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MockTokenStreamHelper {
    private static final Logger LOG = LoggerFactory.getLogger(MockTokenStreamHelper.class);

    final Queue<Token> tokenQueue = new LinkedBlockingQueue<>();

    public MockTokenStreamHelper(final Integer ... types) {
        for (final Integer type : types) {
            addToken(type);
        }
    }

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

    public Object LA(final InvocationOnMock invocation) {
        return ((Token) LT(invocation)).getType();
    }
}
