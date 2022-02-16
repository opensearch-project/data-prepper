/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;

/**
 * @since 1.3
 * Used to assert a leaf node in a ParseTree is an instance of {@link TerminalNode}
 */
public class TerminalNodeMatcher extends DiagnosingMatcher<ParseTree> {
    /**
     * @since 1.3
     * <p>Shortcut for constructor matching Hamcrest standard.</p>
     * <p>
     *     <b>Syntax</b><br>
     *     <pre>assertThat(parseTree, isTerminalNode())</pre>
     * </p>
     *
     * <p>Frequently used with {@link ContextMatcher#hasContext(Class, DiagnosingMatcher[])} and
     * {@link ContextMatcherFactory#isParseTree(Class[])}</p>
     *
     * @return matcher instance
     */
    public static DiagnosingMatcher<ParseTree> isTerminalNode() {
        return new TerminalNodeMatcher();
    }

    /**
     * @since 1.3
     * Asserts ParseTree node is instance of {@link TerminalNode}
     * @param item instance of {@link TerminalNode}
     * @param mismatch Description used for printing Hamcrest mismatch messages
     * @return true if all assertions pass
     */
    @Override
    protected boolean matches(final Object item, final Description mismatch) {
        if (item instanceof TerminalNode) {
            return true;
        }
        else {
            mismatch.appendDescriptionOf(this);
            return false;
        }
    }

    /**
     * @since 1.3
     * Called by Hamcrest when match fails to print useful mismatch error message
     * @param description Where output is collected
     */
    @Override
    public void describeTo(final Description description) {
        description.appendText("Expected object is instance of node");
    }
}
