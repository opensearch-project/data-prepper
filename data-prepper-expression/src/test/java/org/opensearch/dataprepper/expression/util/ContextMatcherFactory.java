/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;

import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class ContextMatcherFactory {

    @SafeVarargs
    public static ContextMatcherFactory isParseTree(final Class<? extends ParseTree> ... types) {
        return new ContextMatcherFactory(types);
    }

    final Class<? extends ParseTree>[] types;

    @SafeVarargs
    private ContextMatcherFactory(final Class<? extends ParseTree> ... types) {
        this.types = types;
    }

    @SafeVarargs
    public final DiagnosingMatcher<ParseTree> withChildrenMatching(final DiagnosingMatcher<ParseTree> ... childrenMatchers) {

        final Class<? extends ParseTree> lastType = types[types.length - 1];
        DiagnosingMatcher<ParseTree> result = hasContext(lastType, childrenMatchers);

        for (int x = types.length - 2; x >= 0; x--) {
            result = hasContext(types[x], result);
        }

        return result;
    }

    public DiagnosingMatcher<ParseTree> containingTerminalNode() {
        return withChildrenMatching(isTerminalNode());
    }
}
