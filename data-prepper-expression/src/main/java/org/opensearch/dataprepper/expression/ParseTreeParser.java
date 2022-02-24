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

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles interaction with ANTLR generated parser and lexer classes and caches results.
 */
@Named
class ParseTreeParser implements Parser<ParseTree> {
    private final Map<String, ParseTree> cache = new HashMap<>();
    private final ParserErrorListener errorListener;
    private final Lexer lexer;
    private final DataPrepperExpressionParser parser;

    @Inject
    public ParseTreeParser(final DataPrepperExpressionParser parser, final ParserErrorListener errorListener) {
        this.parser = parser;
        this.errorListener = errorListener;
        this.parser.addErrorListener(errorListener);

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
     * Converts an expression to a parse tree base on grammar rules
     *
     * @param expression String to be parsed
     * @return ParseTree data structure containing a hierarchy of the tokens found while parsing.
     * @throws ParsingExceptions thrown when ANTLR parser creates an exception event
     */
    private ParseTree createParseTree(final String expression) throws ParsingExceptions {
        final IntStream input = CharStreams.fromString(expression);
        lexer.setInputStream(input);

        final TokenStream tokenStream = new CommonTokenStream(lexer);
        parser.setTokenStream(tokenStream);

        final ParseTree parseTree = parser.expression();

        if (errorListener.isErrorFound()) {
            throw new ParsingExceptions(
                    "Unable to parse expression " + expression,
                    errorListener.getExceptions()
            );
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
     * @throws ParsingExceptions thrown when ANTLR parser creates an exception event
     */
    @Override
    public ParseTree parse(final String expression) throws ParsingExceptions {
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
