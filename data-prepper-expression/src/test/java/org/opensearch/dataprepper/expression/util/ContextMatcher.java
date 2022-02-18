/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

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
    public static void describeContextTo(final ParseTree ctx, final Description mismatch) {
        if (ctx != null) {
            final StringBuilder context = new StringBuilder(ctx.getText());
            ParseTree parent = ctx.getParent();

            while (parent != null) {
                context.insert(0, parent.getText() + " -> ");
                parent = parent.getParent();
            }

            mismatch.appendText("\n\t\t" + context + "\n\t\t");
        }
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
            final DiagnosingMatcher<ParseTree>... childrenMatchers
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
     * @param ctx ParseTree branch to get children from
     * @param mismatch Description used for printing Hamcrest mismatch messages
     * @return true if all assertions pass
     */
    private boolean matchChildren(final ParseTree ctx, final Description mismatch) {
        if (listSizeMatcher.matches(ctx.getChildCount())) {
            for (int i = 0; i < childrenMatchers.length; i++) {
                final ParseTree child = ctx.getChild(i);
                final DiagnosingMatcher<? extends ParseTree> matcher = childrenMatchers[i];

                if (!matcher.matches(child)) {
                    mismatch.appendDescriptionOf(matcher)
                            .appendText(" ");
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
            listSizeMatcher.describeMismatch(ctx.getChildCount(), mismatch);
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
            final ParseTree ctx = (ParseTree) item;
            return matchChildren(ctx, mismatch);
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
