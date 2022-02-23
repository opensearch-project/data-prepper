/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.ErrorListener;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ParserTest {

    private ErrorListener errorListener;

    private void parseExpression(final String expression) {
        errorListener = new ErrorListener();

        final CodePointCharStream stream = CharStreams.fromString(expression);
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(stream);
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        final DataPrepperExpressionParser parser = new DataPrepperExpressionParser(tokenStream);
        parser.addErrorListener(errorListener);

        final ParserRuleContext context = parser.expression();

        final ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(errorListener, context);
    }

    private void assertThatIsValid(final String expression) {
        parseExpression(expression);
        assertThat(
                "\"" + expression + "\" should not have parsing errors",
                errorListener.isErrorFound(),
                is(false)
        );
    }

    private void assertThatHasParseError(final String expression) {
        parseExpression(expression);
        assertThat(
                "\"" + expression + "\" should have parsing errors",
                errorListener.isErrorFound(),
                is(true)
        );
    }

    @Test
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        assertThatHasParseError("(true) =~ \"Hello?\"");
    }

    @Test
    void testInvalidSetItem() {
        assertThatHasParseError("{true or false}");
        assertThatHasParseError("{(5)}");
        assertThatHasParseError("{5 == 5}");
        assertThatHasParseError("{/node =~ \"[A-Z]*\"}");
    }

    @Test
    void testInvalidSetOperator() {
        assertThatHasParseError("1 in 1");
    }

    @Test
    void testInvalidRegexOperand() {
        assertThatHasParseError("\"foo\"=~3.14");
    }

    @Test
    void testInvalidFloatParsingRules() {
        assertThatHasParseError("0.");
        assertThatHasParseError("00.");
        assertThatHasParseError(".00");
        assertThatHasParseError(".10");
        assertThatHasParseError("1.10");
    }

    @Test
    void testInvalidNumberOfOperands() {
        assertThatHasParseError("5==");
        assertThatHasParseError("==");
        assertThatHasParseError("not");
    }

    @Test
    void testValidOperatorsRequireSpace() {
        assertThatIsValid("/status in {200}");
        assertThatIsValid("/a==(/b==200)");
        assertThatIsValid("/a in {200}");
        assertThatIsValid("/a not in {400}");
        assertThatIsValid("/a<300 and /b>200");
    }

    @Test
    void testInvalidSetOperatorArgs() {
        assertThatHasParseError("/a in ({200})");
        assertThatHasParseError("/a in 5");
        assertThatHasParseError("/a in 3.14");
        assertThatHasParseError("/a in false");
        assertThatHasParseError("\"Hello\" not in ({200})");
        assertThatHasParseError("\"Hello\" not in 5");
        assertThatHasParseError("\"Hello\" not in 3.14");
        assertThatHasParseError("\"Hello\" not in false");
    }

    @Test
    void testInvalidOperatorsRequireSpace() {
        assertThatHasParseError("/status in{200}");
        assertThatHasParseError("/status in({200})");
        assertThatHasParseError("/a in{200, 202}");
        assertThatHasParseError("/a not in{400}");
        assertThatHasParseError("/b<300and/b>200");
        assertThatHasParseError("/a in {200,}");
    }

    @Test
    void testValidOptionalSpaceOperators() {
        assertThatIsValid("/status < 300");
        assertThatIsValid("/status>=300");
        assertThatIsValid("/msg =~ \"^\\\\w*\\$\"");
        assertThatIsValid("/msg=~\"[A-Za-z]\"");
        assertThatIsValid("/status == 200");
        assertThatIsValid("/status_code==200");
        assertThatIsValid("/a in {200, 202}");
        assertThatIsValid("/a in {200,202}");
        assertThatIsValid("/a in {200 , 202}");
    }
}
