/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;

public class TerminalNodeMatcher extends DiagnosingMatcher<ParseTree> {
    public static DiagnosingMatcher<ParseTree> isTerminalNode() {
        return new TerminalNodeMatcher();
    }

    @Override
    protected boolean matches(final Object item, final Description mismatchDescription) {
        if (item instanceof TerminalNode) {
            return true;
        }
        else {
            mismatchDescription.appendDescriptionOf(this);
            return false;
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Expected object is instance of node");
    }
}
