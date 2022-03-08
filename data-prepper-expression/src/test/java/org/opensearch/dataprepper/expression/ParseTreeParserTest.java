/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ParseTreeParserTest {
    private static final String VALID_STATEMENT = "Valid Statement";

    @Mock
    DataPrepperExpressionParser parser;

    @Mock
    TokenStream tokenStream;

    @Mock
    Lexer lexer;

    @Mock
    ParserErrorListener errorListener;

    ParseTreeParser parseTreeParser;

    @BeforeEach
    void beforeEach() {
        doReturn(tokenStream).when(parser).getTokenStream();
        doReturn(lexer).when(tokenStream).getTokenSource();
        doReturn(Collections.singletonList(errorListener)).when(parser).getErrorListeners();

        parseTreeParser = new ParseTreeParser(parser);
    }

    @Test
    void testMissingLexer() {
        final DataPrepperExpressionParser mockParser = mock(DataPrepperExpressionParser.class);
        doReturn(mock(TokenStream.class)).when(mockParser).getTokenStream();
        doReturn(Collections.singletonList(errorListener)).when(mockParser).getErrorListeners();

        assertThrows(ClassCastException.class, () -> new ParseTreeParser(mockParser));
    }

    @Test
    void testMissingListener() {
        final DataPrepperExpressionParser mockParser = mock(DataPrepperExpressionParser.class);

        assertThrows(IllegalStateException.class, () -> new ParseTreeParser(mockParser));
    }

    @Test
    void testValidStatement() throws ParseTreeCompositeException {
        final ParseTree expected = mock(DataPrepperExpressionParser.ExpressionContext.class);
        doReturn(expected).when(parser).expression();

        final ParseTree parseTree = parseTreeParser.parse(VALID_STATEMENT);

        assertThat(parseTree, is(expected));
        verify(parser).expression();
    }

    @Test
    void testCacheGivenMultipleExpressionCalls() throws ParseTreeCompositeException {
        final ParseTree expected = mock(DataPrepperExpressionParser.ExpressionContext.class);
        doReturn(expected).when(parser).expression();

        ParseTree parseTree = parseTreeParser.parse(VALID_STATEMENT);
        assertThat(parseTree, is(expected));

        parseTree = parseTreeParser.parse(VALID_STATEMENT);
        assertThat(parseTree, is(expected));

        verify(errorListener).isErrorFound();

        // Verify parser.expression() called 1 time
        verify(parser).expression();
    }

    @Test
    void testExceptionThrowWhenParsingErrorPresent() {
        final RecognitionException recognitionException = mock(RecognitionException.class);
        doReturn(Collections.singletonList(recognitionException)).when(errorListener).getExceptions();
        doReturn(true).when(errorListener).isErrorFound();

        assertThrows(ParseTreeCompositeException.class, () -> parseTreeParser.parse("Error should throw"));
    }

    @Test
    void testResetErrorsIsCalled() throws ParseTreeCompositeException {
        final ParseTree expected = mock(DataPrepperExpressionParser.ExpressionContext.class);
        doReturn(expected).when(parser).expression();

        ParseTree parseTree = parseTreeParser.parse(VALID_STATEMENT);
        assertThat(parseTree, is(expected));
        verify(errorListener).isErrorFound();

        parseTree = parseTreeParser.parse("true == true");
        assertThat(parseTree, is(expected));
        verify(errorListener, times(2)).isErrorFound();
    }

}
