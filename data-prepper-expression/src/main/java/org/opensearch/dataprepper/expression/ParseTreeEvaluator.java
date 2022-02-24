/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Named
public class ParseTreeEvaluator implements Evaluator<ParseTree, Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseTreeEvaluator.class);

    private final ParseTreeEvaluatorListener listener;
    private final ParseTreeWalker walker;

    @Inject
    public ParseTreeEvaluator(ParseTreeEvaluatorListener listener, ParseTreeWalker walker) {
        this.listener = listener;
        this.walker = walker;
    }

    @Override
    public Boolean evaluate(ParseTree parseTree, Event event) throws ClassCastException {
        try {
            listener.initialize(event);
            walker.walk(listener, parseTree);
            // TODO: check result type
            return (Boolean) listener.getResult();
        } catch (final Exception e) {
            // TODO: handle exception based on onEvaluateException config value
            LOG.error("Unable to evaluate event", e);
            return null;
        }
    }

    public static void main(String[] args) {
        ScriptParser parser = new ScriptParser();
        ParseTree parseTree = parser.parse("/b == 1");
        ParseTreeWalker walker = new ParseTreeWalker();
        List<Operator<?>> operators = Arrays.asList(
                new AndOperator(), new OrOperator(),
                new InOperator(), new NotInOperator(),
                new EqualOperator(), new NotEqualOperator(),
                new GreaterThanOperator(), new GreaterThanOrEqualOperator(),
                new LessThanOperator(), new LessThanOrEqualOperator(),
                new RegexEqualOperator(), new RegexNotEqualOperator(),
                new NotOperator()
        );
        ParseTreeEvaluatorListener listener = new ParseTreeEvaluatorListener(operators, new CoercionService());
        ParseTreeEvaluator evaluator = new ParseTreeEvaluator(listener, walker);
        JacksonEvent event = JacksonEvent.builder().withEventType("event").withData(Map.of("a", 1)).build();
        System.out.println(evaluator.evaluate(parseTree, event));
    }
}
