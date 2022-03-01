/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class RuleClassOrderedListTest {

    private RuleClassOrderedList ruleClassOrderedList;

    private abstract static class A implements ParseTree {}
    private abstract static class A1 extends A {}
    private abstract static class B implements ParseTree {}
    private abstract static class C implements ParseTree {}

    @BeforeEach
    void beforeEach() {
        ruleClassOrderedList = new RuleClassOrderedList(A.class, B.class, C.class);
    }

    private static Stream<Arguments> provideParseTreesAndIndices() {
        return Stream.of(
                Arguments.of(mock(A.class), 0),
                Arguments.of(mock(A1.class), 0),
                Arguments.of(mock(B.class), 1),
                Arguments.of(mock(C.class), 2)
        );
    }

    @ParameterizedTest
    @MethodSource("provideParseTreesAndIndices")
    void indexOf(final ParseTree parseTree, final Integer index) {
        assertThat(ruleClassOrderedList.indexOf(parseTree), is(index));
    }

    private static Stream<Arguments> provideSequentialParseTrees() {
        return Stream.of(
                Arguments.of(mock(A.class), mock(B.class), true),
                Arguments.of(mock(A1.class), mock(B.class), true),
                Arguments.of(mock(A.class), mock(A1.class), false),
                Arguments.of(mock(B.class), mock(C.class), true),
                Arguments.of(mock(C.class), mock(B.class), false),
                Arguments.of(mock(B.class), mock(B.class), false)
        );
    }

    @ParameterizedTest
    @MethodSource("provideSequentialParseTrees")
    void isSequentialRules(final ParseTree first, final ParseTree second, final boolean isSequential) {
        assertThat(ruleClassOrderedList.isSequentialRules(first, second), is(isSequential));
    }

    @Test
    void isInstanceOfLastGivenInstanceOfLast() {
        assertThat(ruleClassOrderedList.isInstanceOfLast(mock(C.class)), is(true));
    }

    @Test
    void isInstanceOfLastGivenNotInstanceOfLast() {
        assertThat(ruleClassOrderedList.isInstanceOfLast(mock(B.class)), is(false));
    }
}