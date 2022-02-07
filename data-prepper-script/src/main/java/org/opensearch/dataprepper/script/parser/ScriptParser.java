/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptLexer;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptParser {
    private static final Logger LOG = LoggerFactory.getLogger(ScriptParser.class);
    private static final CharStream EMPTY_STREAM = CharStreams.fromString("");

    private final DataPrepperScriptLexer lexer;
    private final DataPrepperScriptParser parser;

    public ScriptParser() {
        lexer = new DataPrepperScriptLexer(EMPTY_STREAM);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        parser = new DataPrepperScriptParser(tokenStream);
    }

    public ParseTree parse(final String statement) {
        LOG.debug("Parsing statement: {}", statement);

        final IntStream input = CharStreams.fromString(statement);
        lexer.setInputStream(input);

        final TokenStream tokenStream = new CommonTokenStream(lexer);
        parser.setInputStream(tokenStream);

        return parser.statement();
    }
}
