/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

/**
 * @since 1.3
 *
 * <p>
 *     ContextMatcher is a custom Hamcrest matcher to assert if a {@link ParseTree} is an instance of the expected
 *     context and assert child node types and count. Should be used with {@link TerminalNodeMatcher}.
 * </p>
 * <p>
 *     <b>Example</b><br>
 *     Given tree:
 *     <pre>
 *     Expression<br>
 *     ├─ ConditionalExpression<br>
 *     │  ├─ EqualityOperatorExpression<br>
 *     ├─ &lt;EOF&gt;<br>
 *     </pre>
 *
 *     Matcher Assertion
 *     <pre>
 *         assertThat(parseTree, hasContext(Expression,<br>
 *             hasContext(ConditionalExpression, hasContext(EqualityOperatorExpression)),<br>
 *             hasContext(isTerminalNode())<br>
 *         ))
 *     </pre>
 * </p>
 */
public class ContextMatcher extends DiagnosingMatcher<ParseTree> {

    /**
     * Converts long antlr class names to easily read format. Class names without '$' will not be formatted.
     * @param object source of class name to format
     * @return formatted string
     */
    protected static String shortClassString(final Object object) {
        final String classString = object.getClass().toString();
        final int endOfPrefix = classString.indexOf('$');

        if (endOfPrefix >= 0) {
            return classString.substring(endOfPrefix + 1);
        }
        else {
            return classString;
        }
    }

    /**
     * Creates a string of a parse tree and all parent nodes to give context on where in a tree a node is located.
     * @param parseTree node to generate location from
     * @param mismatch Hamcrest Description where context will be appended.
     */
    protected static void describeContextTo(final ParseTree parseTree, final Description mismatch) {
        if (parseTree != null) {
            final StringBuilder context = new StringBuilder(parseTree.getText() + " | " + shortClassString(parseTree));
            ParseTree parent = parseTree.getParent();

            while (parent != null) {
                context.insert(0, parent.getText() + " | " + shortClassString(parent) + "\n\t\t-> ");
                parent = parent.getParent();
            }

            mismatch.appendText("\n\t\t" + context + "\n\t\t");
        }
    }

    /**
     * Creates matcher to check if tree is <b>operatorType</b> with a single child of type TerminalNode
     * @param operatorType class type of operator to assert
     * @return DiagnosingMatcher
     */
    public static DiagnosingMatcher<ParseTree> isOperator(final Class<? extends ParseTree> operatorType) {
        return hasContext(operatorType, isTerminalNode());
    }

    /**
     * @since 1.3
     *
     * <p>Creates a matcher to check for standard ParseTree root.</p>
     * <pre>
     * Expression<br>
     * ├─ {lhs}<br>
     * ├─ &lt;EOF&gt;<br>
     * </pre>
     *
     * @param lhs matcher to use for the first child of the Expression node
     * @return DiagnosingMatcher
     *
     * @see ContextMatcher#hasContext(Class, DiagnosingMatcher[])
     */
    public static DiagnosingMatcher<ParseTree> isExpression(final DiagnosingMatcher<ParseTree> lhs) {
        return hasContext(DataPrepperExpressionParser.ExpressionContext.class, lhs, isTerminalNode());
    }

    /**
     * @since 1.3
     * <p>Shortcut for constructor matching Hamcrest standard.</p>
     * <p>
     *     <b>Syntax</b><br>
     *     <pre>assertThat(parseTree, hasContext(Expression, [child assertions]))</pre>
     * </p>
     * @param parserRuleContextType used to assert ParseTree branch is instance of parserRuleContextType
     * @param childrenMatchers assertions to be used on child nodes. Matcher will also assert order and count
     * @return matcher instance
     */
    @SafeVarargs
    public static DiagnosingMatcher<ParseTree> hasContext(
            final Class<? extends ParseTree> parserRuleContextType,
            final DiagnosingMatcher<? extends ParseTree>... childrenMatchers
    ) {
        return new ContextMatcher(parserRuleContextType, childrenMatchers);
    }

    private final DiagnosingMatcher<? extends ParseTree>[] childrenMatchers;
    final Matcher<? extends ParseTree> isParserRuleContextType;
    private final Matcher<Integer> listSizeMatcher;
    @Nullable
    private Matcher<?> failedAssertion;

    @SafeVarargs
    public ContextMatcher(
            final Class<? extends ParseTree> parserRuleContextType,
            final DiagnosingMatcher<? extends ParseTree> ... childrenMatchers
    ) {
        this.childrenMatchers = childrenMatchers;
        isParserRuleContextType = is(instanceOf(parserRuleContextType));
        listSizeMatcher = equalTo(childrenMatchers.length);
    }

    /**
     * @since 1.3
     * Asserts number of children equal to the number of childMatchers and, in order each child matches the
     * corresponding matcher.
     * @param parseTree ParseTree branch to get children from
     * @param mismatch Description used for printing Hamcrest mismatch messages
     * @return true if all assertions pass
     */
    private boolean matchChildren(final ParseTree parseTree, final Description mismatch) {
        if (listSizeMatcher.matches(parseTree.getChildCount())) {
            for (int i = 0; i < childrenMatchers.length; i++) {
                final ParseTree child = parseTree.getChild(i);
                final DiagnosingMatcher<? extends ParseTree> matcher = childrenMatchers[i];

                if (!matcher.matches(child)) {
                    mismatch.appendText("Expected context \"" + child.getText() + "\"");
                    mismatch.appendText(" | " + shortClassString(child));
                    mismatch.appendText(" to match ");
                    mismatch.appendDescriptionOf(matcher);
                    mismatch.appendText("\n\t\t");
                    matcher.describeMismatch(child, mismatch);
                    failedAssertion = matcher;
                    return false;
                }
            }

            return true;
        }
        else {
            mismatch.appendDescriptionOf(listSizeMatcher)
                    .appendText(" ");
            listSizeMatcher.describeMismatch(parseTree.getChildCount(), mismatch);
            failedAssertion = listSizeMatcher;
            return false;
        }
    }

    /**
     * @since 1.3
     * Asserts ParseTree branch matches assertion and all children match assertions, if any.
     * @param item ParseTree branch to assert against
     * @param mismatch Description used for printing Hamcrest mismatch messages
     * @return true if all assertions pass
     */
    public boolean matches(final Object item, final Description mismatch) {
        if (isParserRuleContextType.matches(item)) {
            final ParseTree parseTree = (ParseTree) item;
            return matchChildren(parseTree, mismatch);
        }
        else {
            mismatch.appendDescriptionOf(isParserRuleContextType)
                    .appendText("\n\t\tfinally ");
            isParserRuleContextType.describeMismatch(item, mismatch);
            failedAssertion = isParserRuleContextType;
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
        if (failedAssertion != null)
            failedAssertion.describeTo(description);
    }
}
