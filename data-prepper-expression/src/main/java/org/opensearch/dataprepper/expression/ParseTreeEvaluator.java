/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

@Named
class ParseTreeEvaluator implements Evaluator<ParseTree, Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseTreeEvaluator.class);

    private final OperatorProvider operatorProvider;
    private final ParseTreeWalker walker;
    private final ParseTreeCoercionService coercionService;

    @Inject
    public ParseTreeEvaluator(final OperatorProvider operatorProvider, final ParseTreeWalker walker,
                              final ParseTreeCoercionService coercionService) {
        this.operatorProvider = operatorProvider;
        this.walker = walker;
        this.coercionService = coercionService;
    }

    @Override
    public Boolean evaluate(ParseTree parseTree, Event event) {
        try {
            final ParseTreeEvaluatorListener listener = new ParseTreeEvaluatorListener(operatorProvider, coercionService, event);
            walker.walk(listener, parseTree);
            return coercionService.coerce(listener.getResult(), Boolean.class);
        } catch (final Exception e) {
            LOG.error("Unable to evaluate event", e);
            throw new ExpressionEvaluationException(e.getMessage(), e);
        }
    }
}
