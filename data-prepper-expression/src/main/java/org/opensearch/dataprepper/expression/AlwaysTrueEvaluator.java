/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.inject.Named;

@Named
// TODO: Delete this class
public class AlwaysTrueEvaluator implements Evaluator<ParseTree, Event> {
    @Override
    public Object evaluate(final ParseTree parseTree, final Event event) throws ClassCastException {
        return true;
    }
}
