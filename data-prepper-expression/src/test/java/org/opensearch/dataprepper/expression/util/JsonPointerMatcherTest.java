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
import static org.opensearch.dataprepper.expression.util.JsonPointerMatcher.isJsonPointerUnaryTree;

class JsonPointerMatcherTest {

    @Test
    void baseCase() {
        final DiagnosingMatcher<ParseTree> isJsonPointerUnaryTree = isJsonPointerUnaryTree();
        final ParseTree primary = mock(DataPrepperExpressionParser.PrimaryContext.class, "PrimaryContext");
        final ParseTree jsonPointer = mock(DataPrepperExpressionParser.JsonPointerContext.class, "JsonPointerContext");
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

        assertTrue(isJsonPointerUnaryTree.matches(primary));
    }
}