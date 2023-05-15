/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.util.FunctionMatcher.isFunctionUnaryTree;

class FunctionMatcherTest {

    @Test
    void baseCase() {
        final DiagnosingMatcher<ParseTree> isFunctionUnaryTree = isFunctionUnaryTree();
        final ParseTree primary = mock(DataPrepperExpressionParser.PrimaryContext.class, "PrimaryContext");
        final ParseTree jsonPointer = mock(DataPrepperExpressionParser.FunctionContext.class, "FunctionContext");
        final ParseTree terminal = mock(TerminalNode.class, "TerminalNode");

        doReturn(1)
                .when(primary)
                .getChildCount();
        doReturn(jsonPointer)
                .when(primary)
                .getChild(eq(0));
        doReturn(1)
                .when(jsonPointer)
                .getChildCount();
        doReturn(terminal)
                .when(jsonPointer)
                .getChild(eq(0));

        assertTrue(isFunctionUnaryTree.matches(primary));
    }
}

