/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles interaction with ANTLR generated parser and lexer classes and caches results.
 */
@Named(ParseTreeParser.SINGLE_THREAD_PARSER_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class ParseTreeParser implements Parser<ParseTree> {
    static final String SINGLE_THREAD_PARSER_NAME = "singleThreadParser";
    private static final String MISSING_PARSER_ERROR_LISTENER_MESSAGE =
            "Expected DataPrepperExpressionParser to have error listener of type ParserErrorListener but none were found.";
    private final Map<String, ParseTree> cache = new HashMap<>();
    private final ParserErrorListener errorListener;
    private final Lexer lexer;
    private final DataPrepperExpressionParser parser;

    @Inject
    public ParseTreeParser(final DataPrepperExpressionParser parser) {
        this.parser = parser;
        this.errorListener = (ParserErrorListener) this.parser.getErrorListeners()
                .stream()
                .filter(errorListener -> errorListener instanceof ParserErrorListener)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(MISSING_PARSER_ERROR_LISTENER_MESSAGE));

        final TokenSource tokenSource = parser.getTokenStream().getTokenSource();
        if (tokenSource instanceof Lexer) {
            lexer = (Lexer) tokenSource;
        }
        else {
            throw new ClassCastException("Expected DataPrepperStatementParser token source to be instance of Lexer");
        }
    }

    /**
     * @since 1.3
     *
     * Converts an expression to a parse tree base on grammar rules. {@link ParserErrorListener#resetErrors()} must be called
     * before {@link DataPrepperExpressionParser#expression()} to prevent duplicate errors from being reported.
     *
     * @param expression String to be parsed
     * @return ParseTree data structure containing a hierarchy of the tokens found while parsing.
     * @throws ParseTreeCompositeException thrown when ANTLR parser creates an exception event
     */
    private ParseTree createParseTree(final String expression) throws ParseTreeCompositeException {
        errorListener.resetErrors();

        final IntStream input = CharStreams.fromString(expression);
        lexer.setInputStream(input);

        final TokenStream tokenStream = new CommonTokenStream(lexer);
        parser.setTokenStream(tokenStream);

        final ParseTree parseTree = parser.expression();

        if (errorListener.isErrorFound()) {
            throw new ParseTreeCompositeException(errorListener.getExceptions());
        }
        else {
            return parseTree;
        }
    }

    /**
     * @since 1.3
     *
     * Check if cache already has a parse tree available for the given statement. If yes, return from cache otherwise
     * parse expression, cache and return result.
     *
     * @param expression String to be parsed
     * @return ParseTree data structure containing a hierarchy of the tokens found while parsing.
     * @throws ParseTreeCompositeException thrown when ANTLR parser creates an exception event
     */
    @Override
    public ParseTree parse(final String expression) throws ParseTreeCompositeException {
        if (cache.containsKey(expression)) {
            return cache.get(expression);
        }
        else {
            final ParseTree parseTree = createParseTree(expression);
            cache.put(expression, parseTree);
            return parseTree;
        }
    }
}
