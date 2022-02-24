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
        ParseTree parseTree = parser.parse("true == false");
    }
}
