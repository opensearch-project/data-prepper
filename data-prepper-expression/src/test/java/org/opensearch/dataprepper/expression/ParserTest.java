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
import org.junit.jupiter.api.function.Executable;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.ErrorListener;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

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

    private Executable assertThatHasParseError(final String expression) {
        return () -> {
            parseExpression(expression);
            assertThat(
                    "\"" + expression + "\" should have parsing errors",
                    errorListener.isErrorFound(),
                    is(true)
            );
        };
    }

    @Test
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        parseExpression("(true) =~ \"Hello?\"");

        assertThat(errorListener.isErrorFound(), is(true));
    }

    @Test
    void testInvalidSetItem() {
        assertAll(
                assertThatHasParseError("{true or false}"),
                assertThatHasParseError("{(5)}"),
                assertThatHasParseError("{5 == 5}"),
                assertThatHasParseError("{/node =~ \"[A-Z]*\"}")
        );
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
        assertAll(
                assertThatHasParseError("0."),
                assertThatHasParseError("00."),
                assertThatHasParseError(".00"),
                assertThatHasParseError(".10"),
                assertThatHasParseError("1.10")
        );
    }

    @Test
    void testInvalidNumberOfOperands() {
        assertThatHasParseError("5==");
        assertThatHasParseError("==");
        assertThatHasParseError("not");
    }
}
