/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

/**
 * Public class that {@link org.opensearch.dataprepper.model.processor.Processor},
 * {@link org.opensearch.dataprepper.model.sink.Sink} and data-prepper-core objects can use to evaluate statements.
 */
@Named
class GenericExpressionEvaluator implements ExpressionEvaluator {
    private final Parser<ParseTree> parser;
    private final Evaluator<ParseTree, Event> evaluator;

    @Inject
    public GenericExpressionEvaluator(final Parser<ParseTree> parser, final Evaluator<ParseTree, Event> evaluator) {
        this.parser = parser;
        this.evaluator = evaluator;
    }

    /**
     * {@inheritDoc}
     *
     * @throws ExpressionEvaluationException if unable to evaluate or coerce the statement result to type T
     */
    @Override
    public Object evaluate(final String statement, final Event context) {
        try {
            final ParseTree parseTree = parser.parse(statement);
            return evaluator.evaluate(parseTree, context);
        }
        catch (final Exception exception) {
            throw new ExpressionEvaluationException("Unable to evaluate statement \"" + statement + "\"", exception);
        }
    }

    @Override
    public Boolean isValidExpressionStatement(final String statement) {
        try {
            parser.parse(statement);
            return true;
        }
        catch (final Exception exception) {
            return false;
        }
    }

    @Override
    public Boolean isValidFormatExpression(final String format) {
        int fromIndex = 0;
        int position = 0;
        while ((position = format.indexOf("${", fromIndex)) != -1) {
            int endPosition = format.indexOf("}", position + 1);
            if (endPosition == -1) {
                return false;
            }
            String name = format.substring(position + 2, endPosition);

            Object val;
            // Invalid if it is not a valid key and not a valid expression statement
            if (!JacksonEvent.isValidEventKey(name) && !isValidExpressionStatement(name)) {
                return false;
            }
            fromIndex = endPosition + 1;
        }
        return true;
    }

    @Override
    public List<String> extractDynamicKeysFromFormatExpression(final String format) {
        final List<String> formatExpressionKeys = new ArrayList<>();

        if (format == null) {
            return formatExpressionKeys;
        }

        int fromIndex = 0;
        int position = 0;
        while ((position = format.indexOf("${", fromIndex)) != -1) {
            int endPosition = format.indexOf("}", position + 1);
            if (endPosition == -1) {
                return formatExpressionKeys;
            }
            String name = format.substring(position + 2, endPosition);

            if (JacksonEvent.isValidEventKey(name)) {
                if (!name.startsWith("/")) {
                    name = "/" + name;
                }
                formatExpressionKeys.add(name);
            }
            fromIndex = endPosition + 1;
        }
        return formatExpressionKeys;
    }

    @Override
    public List<String> extractDynamicExpressionsFromFormatExpression(final String format) {
        final List<String> dynamicExpressionStatements = new ArrayList<>();

        if (format == null) {
            return dynamicExpressionStatements;
        }

        int fromIndex = 0;
        int position = 0;
        while ((position = format.indexOf("${", fromIndex)) != -1) {
            int endPosition = format.indexOf("}", position + 1);
            if (endPosition == -1) {
                return dynamicExpressionStatements;
            }
            String name = format.substring(position + 2, endPosition);

            if (!JacksonEvent.isValidEventKey(name) && isValidExpressionStatement(name)) {
                dynamicExpressionStatements.add(name);
            }
            fromIndex = endPosition + 1;
        }
        return dynamicExpressionStatements;
    }
}
