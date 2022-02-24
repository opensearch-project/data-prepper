/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

public class GrammarLexerTest {

    private List<? extends Token> getTokens(final String statement) {
        final Lexer lexer = new DataPrepperExpressionLexer(CharStreams.fromString(statement));
        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();
        return tokenStream.getTokens();
    }

    private void assertToken(final String statement, final int type) {
        final List<? extends Token> tokens = getTokens(statement);

        assertThat(tokens.size(), is(2));

        assertAll(
                () -> assertThat(tokens.get(0).getType(), is(type)),
                () -> assertThat(tokens.get(0).getText(), is(statement)),
                () -> assertThat(tokens.get(1).getType(), is(DataPrepperExpressionLexer.EOF))
        );
    }

    @Test
    void testTokenInteger() {
        assertToken("5", DataPrepperExpressionLexer.Integer);
    }

    @Test
    void testTokenFloat() {
        assertToken("3.14", DataPrepperExpressionLexer.Float);
    }

    @Test
    void testTokenBoolean() {
        assertToken("true", DataPrepperExpressionLexer.Boolean);
    }

    @Test
    void testTokenJsonPointer() {
        assertToken("/status_code", DataPrepperExpressionLexer.JsonPointer);
    }

    @Test
    void testTokenString() {
        assertToken("\"Hello World\"", DataPrepperExpressionLexer.String);
    }

    @Test
    void testTokenVariableIdentifier() {
        assertToken("${var}", DataPrepperExpressionLexer.VariableIdentifier);
    }

    @Test
    void testTokenEQUAL() {
        assertToken("==", DataPrepperExpressionLexer.EQUAL);
    }

    @Test
    void testTokenNOT_EQUAL() {
        assertToken("!=", DataPrepperExpressionLexer.NOT_EQUAL);
    }

    @Test
    void testTokenLT() {
        assertToken("<", DataPrepperExpressionLexer.LT);
    }

    @Test
    void testTokenGT() {
        assertToken(">", DataPrepperExpressionLexer.GT);
    }

    @Test
    void testTokenLTE() {
        assertToken("<=", DataPrepperExpressionLexer.LTE);
    }

    @Test
    void testTokenGTE() {
        assertToken(">=", DataPrepperExpressionLexer.GTE);
    }

    @Test
    void testTokenMATCH_REGEX_PATTERN() {
        assertToken("=~", DataPrepperExpressionLexer.MATCH_REGEX_PATTERN);
    }

    @Test
    void testTokenNOT_MATCH_REGEX_PATTERN() {
        assertToken("!~", DataPrepperExpressionLexer.NOT_MATCH_REGEX_PATTERN);
    }

    @Test
    void testTokenIN_SET() {
        assertToken(" in ", DataPrepperExpressionLexer.IN_SET);
    }

    @Test
    void testTokenNOT_IN_SET() {
        assertToken(" not in ", DataPrepperExpressionLexer.NOT_IN_SET);
    }

    @Test
    void testTokenAND() {
        assertToken(" and ", DataPrepperExpressionLexer.AND);
    }

    @Test
    void testTokenOR() {
        assertToken(" or ", DataPrepperExpressionLexer.OR);
    }

    @Test
    void testTokenNOT() {
        assertToken("not ", DataPrepperExpressionLexer.NOT);
    }

    @Test
    void testTokenLPAREN() {
        assertToken("(", DataPrepperExpressionLexer.LPAREN);
    }

    @Test
    void testTokenRPAREN() {
        assertToken(")", DataPrepperExpressionLexer.RPAREN);
    }

    @Test
    void testTokenLBRACE() {
        assertToken("{", DataPrepperExpressionLexer.LBRACE);
    }

    @Test
    void testTokenRBRACE() {
        assertToken("}", DataPrepperExpressionLexer.RBRACE);
    }

    @Test
    void testTokenDOUBLEQUOTE() {
        assertToken("\"", DataPrepperExpressionLexer.DOUBLEQUOTE);
    }

    @Test
    void testTokenFORWARDSLASH() {
        assertToken("/", DataPrepperExpressionLexer.FORWARDSLASH);
    }

    @Test
    void testSpaceInsignificant() {
        final String statement = " ";
        final List<? extends Token> tokens = getTokens(statement);

        assertThat(tokens.size(), is(1));
        assertAll(
                () -> assertThat(tokens.get(0).getType(), is(DataPrepperExpressionLexer.EOF)),
                () -> assertThat(tokens.get(0).getText(), is("<EOF>"))
        );
    }
}
