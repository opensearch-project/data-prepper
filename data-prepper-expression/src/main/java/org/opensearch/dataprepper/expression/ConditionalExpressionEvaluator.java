/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Public class that {@link com.amazon.dataprepper.model.processor.Processor},
 * {@link com.amazon.dataprepper.model.sink.Sink} and data-prepper-core objects can use to evaluate statements.
 */
@Named
class ConditionalExpressionEvaluator implements ExpressionEvaluator<Boolean> {
    private final Parser<ParseTree> parser;
    private final Evaluator<ParseTree, Event> evaluator;

    @Inject
    public ConditionalExpressionEvaluator(final Parser<ParseTree> parser, final Evaluator<ParseTree, Event> evaluator) {
        this.parser = parser;
        this.evaluator = evaluator;
    }

    /**
     * {@inheritDoc}
     *
     * @throws ExpressionEvaluationException if unable to evaluate or coerce the statement result to type T
     */
    @Override
    public Boolean evaluate(final String statement, final Event context) {
        try {
            final ParseTree parseTree = parser.parse(statement);
            final Object result = evaluator.evaluate(parseTree, context);

            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            else {
                throw new ClassCastException("Unexpected expression return type of " + result.getClass());
            }
        }
        catch (final Exception exception) {
            throw new ExpressionEvaluationException("Unable to evaluate statement \"" + statement + "\"", exception);
        }
    }
}
