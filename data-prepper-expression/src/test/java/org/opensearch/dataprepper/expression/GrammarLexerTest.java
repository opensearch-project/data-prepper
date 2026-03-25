/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertAll;

class GrammarLexerTest {

    private List<? extends Token> getTokens(final String statement) {
        final Lexer lexer = new DataPrepperExpressionLexer(CharStreams.fromString(statement));
        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();
        return tokenStream.getTokens();
    }

    private void assertIsNotToken(final String statement, final int type) {
        final List<? extends Token> tokens = getTokens(statement);

        assertThat(tokens.size(), is(greaterThanOrEqualTo(2)));
        assertThat(tokens.get(0).getType(), is(not(type)));
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
        assertToken("12345.678", DataPrepperExpressionLexer.Float);
        assertToken("12345.678E-15", DataPrepperExpressionLexer.Float);
        assertToken("12345.678E33", DataPrepperExpressionLexer.Float);
        assertToken("12345.678e-153", DataPrepperExpressionLexer.Float);
        assertToken("12345.678e11", DataPrepperExpressionLexer.Float);
        assertToken("0.6782", DataPrepperExpressionLexer.Float);
        assertToken("0.678E12", DataPrepperExpressionLexer.Float);
        assertToken("0.678E-12", DataPrepperExpressionLexer.Float);
        assertToken("0.678e-12", DataPrepperExpressionLexer.Float);
        assertToken("0.678e12", DataPrepperExpressionLexer.Float);
        assertToken("6782.0", DataPrepperExpressionLexer.Float);
        assertToken("12345678.000002", DataPrepperExpressionLexer.Float);
        assertToken("12345678.0002e6", DataPrepperExpressionLexer.Float);
        assertToken("12345678.000252E16", DataPrepperExpressionLexer.Float);
        // only one zero before the decimal point
        assertIsNotToken("0000.678e12", DataPrepperExpressionLexer.Float);
        // Must have one digit before the decimal point
        assertIsNotToken(".678e12", DataPrepperExpressionLexer.Float);
        assertIsNotToken(".678e-12", DataPrepperExpressionLexer.Float);
        assertIsNotToken(".6782", DataPrepperExpressionLexer.Float);
        // Can't end with decimal point
        assertIsNotToken("6782.", DataPrepperExpressionLexer.Float);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void testTokenBoolean(final String booleanStatement) {
        assertToken(booleanStatement, DataPrepperExpressionLexer.Boolean);
    }

    @Test
    void testTokenJsonPointer() {
        assertToken("/status_code", DataPrepperExpressionLexer.JsonPointer);
    }

    @Test
    void testTokenEscapedJsonPointer() {
        assertToken("\"/status_code\"", DataPrepperExpressionLexer.EscapedJsonPointer);
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
    void testTokenSUBTRACT() {
        assertToken("-", DataPrepperExpressionLexer.SUBTRACT);
    }

    @ParameterizedTest
    @ValueSource(strings = {"integer", "boolean", "big_decimal", "long", "double", "string", "map", "array"})
    void testTokenDataTypes(final String dataType) {
        assertToken(dataType, DataPrepperExpressionLexer.DataTypes);
    }

    @ParameterizedTest
    @ValueSource(strings = {"length", "contains", "cidrContains", "hasTags", "getMetadata", "getEventType"})
    void testTokenFunctionName(final String functionName) {
        assertToken(functionName, DataPrepperExpressionLexer.FunctionName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"integer", "boolean", "big_decimal", "long", "double", "string", "map", "array"})
    void testTypeOfExpressionTokenization(final String dataType) {
        final String statement = "/status typeof " + dataType;
        final List<? extends Token> tokens = getTokens(statement);

        assertThat(tokens.size(), is(4));
        assertAll(
                () -> assertThat(tokens.get(0).getType(), is(DataPrepperExpressionLexer.JsonPointer)),
                () -> assertThat(tokens.get(1).getType(), is(DataPrepperExpressionLexer.TYPEOF)),
                () -> assertThat(tokens.get(2).getType(), is(DataPrepperExpressionLexer.DataTypes)),
                () -> assertThat(tokens.get(2).getText(), is(dataType)),
                () -> assertThat(tokens.get(3).getType(), is(DataPrepperExpressionLexer.EOF))
        );
    }

    @Test
    void testFunctionWithNoArgsTokenization() {
        final List<? extends Token> tokens = getTokens("functionWithoutArguments()");

        assertThat(tokens.size(), is(4));
        assertAll(
                () -> assertThat(tokens.get(0).getType(), is(DataPrepperExpressionLexer.FunctionName)),
                () -> assertThat(tokens.get(0).getText(), is("functionWithoutArguments")),
                () -> assertThat(tokens.get(1).getType(), is(DataPrepperExpressionLexer.LPAREN)),
                () -> assertThat(tokens.get(2).getType(), is(DataPrepperExpressionLexer.RPAREN)),
                () -> assertThat(tokens.get(3).getType(), is(DataPrepperExpressionLexer.EOF))
        );
    }

    @Test
    void testFunctionWithArgsTokenization() {
        final List<? extends Token> tokens = getTokens("functionWithTwoArguments(/sourceIp,\"192.0.2.0/24\")");

        assertThat(tokens.size(), is(7));
        assertAll(
                () -> assertThat(tokens.get(0).getType(), is(DataPrepperExpressionLexer.FunctionName)),
                () -> assertThat(tokens.get(0).getText(), is("functionWithTwoArguments")),
                () -> assertThat(tokens.get(1).getType(), is(DataPrepperExpressionLexer.LPAREN)),
                () -> assertThat(tokens.get(2).getType(), is(DataPrepperExpressionLexer.JsonPointer)),
                () -> assertThat(tokens.get(2).getText(), is("/sourceIp")),
                () -> assertThat(tokens.get(3).getType(), is(DataPrepperExpressionLexer.COMMA)),
                () -> assertThat(tokens.get(4).getType(), is(DataPrepperExpressionLexer.String)),
                () -> assertThat(tokens.get(4).getText(), is("\"192.0.2.0/24\"")),
                () -> assertThat(tokens.get(5).getType(), is(DataPrepperExpressionLexer.RPAREN)),
                () -> assertThat(tokens.get(6).getType(), is(DataPrepperExpressionLexer.EOF))
        );
    }

    @Test
    void testTokenNull() {
        assertToken("null", DataPrepperExpressionLexer.Null);
    }

    @Test
    void testTokenCOMMA() {
        assertToken(",", DataPrepperExpressionLexer.COMMA);
    }

    @Test
    void testTokenPLUS() {
        assertToken("+", DataPrepperExpressionLexer.PLUS);
    }

    @Test
    void testTokenMULTIPLY() {
        assertToken("*", DataPrepperExpressionLexer.MULTIPLY);
    }

    @Test
    void testTokenMOD() {
        assertToken("%", DataPrepperExpressionLexer.MOD);
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
