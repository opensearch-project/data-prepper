/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
        final ParseTree functionCtx = mock(DataPrepperExpressionParser.FunctionContext.class, "FunctionContext");
        final ParseTree functionName = mock(TerminalNode.class, "FunctionName");
        final ParseTree lparen = mock(TerminalNode.class, "LPAREN");
        final ParseTree rparen = mock(TerminalNode.class, "RPAREN");

        doReturn(1)
                .when(primary)
                .getChildCount();
        doReturn(functionCtx)
                .when(primary)
                .getChild(eq(0));
        // function is now a parser rule: FunctionName LPAREN RPAREN (3 children, no args)
        doReturn(3)
                .when(functionCtx)
                .getChildCount();
        doReturn(functionName)
                .when(functionCtx)
                .getChild(eq(0));
        doReturn(lparen)
                .when(functionCtx)
                .getChild(eq(1));
        doReturn(rparen)
                .when(functionCtx)
                .getChild(eq(2));

        assertTrue(isFunctionUnaryTree.matches(primary));
    }
}

