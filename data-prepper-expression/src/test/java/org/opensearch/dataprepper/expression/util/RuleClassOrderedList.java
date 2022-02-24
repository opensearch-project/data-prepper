/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * RuleClassOrderedList calculates the position of an objects class or super class in an orders list of classes.
 */
public class RuleClassOrderedList {
    private final List<Class<? extends ParseTree>> orderedRules = new ArrayList<>();

    @SafeVarargs
    public RuleClassOrderedList(final Class<? extends ParseTree> ... rules) {
        Collections.addAll(orderedRules, rules);
    }

    /**
     * Calculates the first index of a class item is an instance of.
     * @param item object that should be an instance of any class within {@link RuleClassOrderedList#orderedRules}
     * @return the index of the class or -1
     */
    public int indexOf(@Nullable final ParseTree item) {
        for (int x = 0; x < orderedRules.size(); x++) {
            if (orderedRules.get(x).isInstance(item)) {
                return x;
            }
        }
        return -1;
    }

    /**
     * Checks if {@link RuleClassOrderedList#indexOf(ParseTree)} of first is immediately followed by the index of second.
     * @param first object that should be an instance of any class within {@link RuleClassOrderedList#orderedRules}
     * @param second object that should be an instance of the class following first in {@link RuleClassOrderedList#orderedRules}
     * @return if the indices are sequential
     */
    public boolean isSequentialRules(@Nullable final ParseTree first, @Nullable final ParseTree second) {
        final int firstIndex = indexOf(first);
        if (firstIndex < 0 || firstIndex >= orderedRules.size() - 1) {
            return false;
        }
        else {
            return orderedRules.get(firstIndex + 1).isInstance(second);
        }
    }

    /**
     * Checks if item is an instance of the last class in {@link RuleClassOrderedList#orderedRules}
     * @param item object to check
     * @return if instanceof last rule class
     */
    public boolean isInstanceOfLast(final ParseTree item) {
        final int lastIndex = orderedRules.size() - 1;
        return orderedRules.get(lastIndex).isInstance(item);
    }
}
