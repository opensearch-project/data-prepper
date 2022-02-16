/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class StubCommonTokenStream extends CommonTokenStream {
    private static final Logger LOG = LoggerFactory.getLogger(StubCommonTokenStream.class);

    final List<Token> tokens = new ArrayList<>();
    private int position = 0;

    public static StubCommonTokenStream stubTokenStream() {
        final CodePointCharStream stream = CharStreams.fromString("");
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(stream);
        return new StubCommonTokenStream(lexer);
    }

    private StubCommonTokenStream(final TokenSource tokenSource) {
        super(tokenSource);
    }

    public void addToken(final int type) {
        final Token token = mock(Token.class);

        doAnswer(invocation -> {
            LOG.info("token.getType() -> {}", type);
            return type;
        }).when(token).getType();

        tokens.add(token);
    }

    @Override
    protected int adjustSeekIndex(final int i) {
        LOG.info("seek({})", i);
        position = i;
        return position;
    }

    @Override
    public void seek(final int index) {
        adjustSeekIndex(index);
    }

    private int clamp(final int i) {
        return Math.max(Math.min(i, tokens.size() - 1), 0);
    }

    @Override
    public Token LT(final int k) {
        LOG.info("LT({})", k);
        try {
            if (k >= 1) {
                return tokens.get(clamp(position));
            }
            else if (k == -1) {
                return tokens.get(clamp(position + k));
            }
            else {
                throw new RuntimeException("Unexpected K = " + k);
            }
        }
        catch (final IndexOutOfBoundsException e) {
            return null;
        }
    }

    @Override
    public int LA(final int i) {
        return LT(i).getType();
    }
}
