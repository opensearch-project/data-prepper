/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperatorProviderTest {

    @Mock
    private Operator<?> testOperator;

    OperatorProvider objectUnderTest;

    @Test
    void testIllegalArgumentsInConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new OperatorProvider(null));
        final int testSymbol = 100;
        final List<Operator<?>> operatorsWithNull = Arrays.asList(testOperator, null);
        assertThrows(IllegalArgumentException.class, () -> new OperatorProvider(operatorsWithNull));
    }

    @Test
    void testContainsOperatorSymbol() {
        final int testSymbol = 100;
        when(testOperator.getSymbol()).thenReturn(testSymbol);
        final List<Operator<?>> testOperators = Collections.singletonList(testOperator);

        objectUnderTest = new OperatorProvider(testOperators);
        assertThat(objectUnderTest.containsOperator(testSymbol), is(true));
        assertThat(objectUnderTest.containsOperator(200), is(false));
    }

    @Test
    void testGetExistingOperator() {
        final int testSymbol1 = 100;
        when(testOperator.getSymbol()).thenReturn(testSymbol1);
        final int testSymbol2 = 200;
        final Operator<?> testOperator2 = mock(Operator.class);
        when(testOperator2.getSymbol()).thenReturn(testSymbol2);
        final List<Operator<?>> testOperators = Arrays.asList(testOperator, testOperator2);

        objectUnderTest = new OperatorProvider(testOperators);
        final Operator<?> retrievedOperator1 =  objectUnderTest.getOperator(testSymbol1);
        assertThat(retrievedOperator1, notNullValue());
        assertThat(retrievedOperator1.getSymbol(), equalTo(testSymbol1));
        objectUnderTest = new OperatorProvider(testOperators);
        final Operator<?> retrievedOperator2 =  objectUnderTest.getOperator(testSymbol2);
        assertThat(retrievedOperator2, notNullValue());
        assertThat(retrievedOperator2.getSymbol(), equalTo(testSymbol2));
    }

    @Test
    void testGetNonExistingOperator() {
        objectUnderTest = new OperatorProvider(Collections.emptyList());
        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.getOperator(100));
    }
}