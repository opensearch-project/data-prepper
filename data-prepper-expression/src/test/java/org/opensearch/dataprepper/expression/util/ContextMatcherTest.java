/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isUnaryTreeSet;

public class ContextMatcherTest {

    @Test
    void testMatchesParseTree() {
        final ContextMatcher matcher = new ContextMatcher(ParseTree.class);
        assertThat(matcher.matches(mock(ParserRuleContext.class)), is(true));
    }

    @Test
    void testNotMatchesObject() {
        final ContextMatcher matcher = new ContextMatcher(ParseTree.class);
        assertThat(matcher.matches(new Object()), is(false));

        final Description description = mock(Description.class);

        doReturn(description)
                .when(description).appendText(anyString());

        matcher.describeTo(description);

        verify(description).appendDescriptionOf(any());
    }

    @Test
    void testMatchesChildren() {
        // mock will fail because mockito cannot identify matcher on doReturn().when().matcher()
        final DiagnosingMatcher<? extends ParseTree> childMatcher = spy(DiagnosingMatcher.class);

        final ContextMatcher matcher = new ContextMatcher(ParseTree.class, childMatcher);

        final ParseTree parseTree = mock(ParserRuleContext.class);
        doReturn(1)
                .doReturn(0)
                .when(parseTree)
                .getChildCount();
        doReturn(parseTree).when(parseTree).getChild(eq(0));

        doReturn(true).when(childMatcher).matches(parseTree);

        assertThat(matcher.matches(parseTree), is(true));
    }

    @Test
    void testStaticConstructor() {
        final DiagnosingMatcher<ParseTree> matcher = hasContext(ParseTree.class);
        assertThat(matcher.matches(mock(ParserRuleContext.class)), is(true));
    }

    @Test
    void testIsUnaryTreeSet() {
        final ParseTree parseTree = mock(DataPrepperExpressionParser.SetInitializerContext.class, "Parse Tree");
        final ParseTree terminal = mock(TerminalNode.class, "Terminal Node");
        final ParseTree literal = mock(DataPrepperExpressionParser.LiteralContext.class, "LiteralNode");
        doReturn(7)
                .when(parseTree)
                .getChildCount();
        doReturn(terminal).when(parseTree).getChild(0);
        doReturn(literal).when(parseTree).getChild(1);
        doReturn(terminal).when(parseTree).getChild(2);
        doReturn(literal).when(parseTree).getChild(3);
        doReturn(terminal).when(parseTree).getChild(4);
        doReturn(literal).when(parseTree).getChild(5);
        doReturn(terminal).when(parseTree).getChild(6);
        doReturn(1).when(literal).getChildCount();
        doReturn(terminal).when(literal).getChild(0);

        final DiagnosingMatcher<ParseTree> unaryTreeSet = isUnaryTreeSet(3);

        assertThat(unaryTreeSet.matches(parseTree), is(true));
    }
}
