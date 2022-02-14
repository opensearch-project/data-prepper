/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParserTest.class);

    @Mock
    private TokenStream tokenStream;
    @InjectMocks
    private DataPrepperExpressionParser parser;

    private Queue<Token> tokens(final Integer ... types) {
        final Queue<Token> tokenQueue = new LinkedBlockingQueue<>();
        Arrays.stream(types)
                .map(type -> {
                    final Token token = mock(Token.class);
//                    doReturn(type)
//                            .when(token).getType();
                    doAnswer(invocation -> {
                        LOG.info("token.getType() -> {}", type);
                        return type;
                    }).when(token).getType();

                    doReturn("Type " + type)
                            .when(token).toString();

                    return token;
                })
                .forEach(tokenQueue::add);
        return tokenQueue;
    }

    private TokenStream mockTokenStream(final Integer ... types) {
        final TokenStream tokenStream = mock(TokenStream.class);
        final Queue<Token> tokenQueue = tokens(types);

        doAnswer(invocation -> {
            LOG.info("Consume !!!!!!!!!!!!!!!");
            if (!tokenQueue.isEmpty()) {
                final Token token = tokenQueue.remove();
                LOG.warn("Popped token {} -> {}, queue has {} items remaining", token, token.getType(), tokenQueue.size());
            }
            else {
                LOG.warn("Consumed empty queue!");
            }
            return null;
        }).when(tokenStream).consume();

//        doReturn(tokenQueue.peek())
        doAnswer(invocation -> {
            final Token token = tokenQueue.peek();
            LOG.info("\t\tTokenStream.LA() type: {} with queue size: {}", token.getType(), tokenQueue.size());
            return token;
        })
                .when(tokenStream).LT(anyInt());

        doAnswer(invocation -> {
            if (tokenQueue.isEmpty()) {
                LOG.info("_input.LA() with empty queue");
                return null;
            }
            else {
                final int type = tokenQueue.peek().getType();
                LOG.info("_input.LA() -> {}", type);
                return type;
            }
        })
                .when(tokenStream).LA(anyInt());

        return tokenStream;
    }

    @Test
    void foo() {
        final TokenStream input = mockTokenStream(
                DataPrepperExpressionParser.Integer,
                DataPrepperExpressionParser.EQUAL,
                DataPrepperExpressionParser.Float,
                DataPrepperExpressionParser.EOF
        );
        parser.setInputStream(input);

        final DataPrepperExpressionParser.ExpressionContext expression = parser.expression();

        assertThat(expression, isA(ParseTree.class));
    }
}
