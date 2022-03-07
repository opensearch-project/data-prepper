/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Named
class ParseTreeEvaluator implements Evaluator<ParseTree, Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseTreeEvaluator.class);

    private final ParseTreeEvaluatorListener listener;
    private final ParseTreeWalker walker;
    private final CoercionService coercionService;

    @Inject
    public ParseTreeEvaluator(final ParseTreeEvaluatorListener listener, final ParseTreeWalker walker,
                              final CoercionService coercionService) {
        this.listener = listener;
        this.walker = walker;
        this.coercionService = coercionService;
    }

    @Override
    public Boolean evaluate(ParseTree parseTree, Event event) {
        try {
            listener.initialize(event);
            walker.walk(listener, parseTree);
            return coercionService.coerce(listener.getResult(), Boolean.class);
        } catch (final Exception e) {
            LOG.error("Unable to evaluate event", e);
            throw new ExpressionEvaluationException(e.getMessage(), e);
        }
    }
}
