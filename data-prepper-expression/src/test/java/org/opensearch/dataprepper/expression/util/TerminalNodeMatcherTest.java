/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

class TerminalNodeMatcherTest {

    @Test
    void testMatchesTerminalNode() {
        final DiagnosingMatcher<ParseTree> matcher = isTerminalNode();
        assertTrue(matcher.matches(mock(TerminalNode.class)));
    }

    @Test
    void testNotMatchesTerminalNode() {
        final DiagnosingMatcher<ParseTree> matcher = new TerminalNodeMatcher();
        assertFalse(matcher.matches(mock(Object.class)));
    }

    @Test
    void testDescribeToAppendsText() {
        final DiagnosingMatcher<ParseTree> matcher = isTerminalNode();
        final Description description = mock(Description.class);

        matcher.describeTo(description);

        verify(description).appendText(anyString());
    }
}
