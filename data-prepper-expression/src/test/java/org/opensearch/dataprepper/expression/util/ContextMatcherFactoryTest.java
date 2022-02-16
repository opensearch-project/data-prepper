/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;

/**
 * @since 1.3
 * Uses JUnit assertions to avoid false failure from Hamcrest
 */
class ContextMatcherFactoryTest {

    @Test
    void testWithChildrenMatching() {
        final ContextMatcherFactory factory = isParseTree(ParseTree.class);
        assertTrue(factory.withChildrenMatching(mock(DiagnosingMatcher.class)) instanceof DiagnosingMatcher);
    }

    @Test
    void testContainingTerminalNode() {
        final ContextMatcherFactory factory = isParseTree(ParseTree.class);
        assertTrue(factory.containingTerminalNode() instanceof DiagnosingMatcher);
    }
}
