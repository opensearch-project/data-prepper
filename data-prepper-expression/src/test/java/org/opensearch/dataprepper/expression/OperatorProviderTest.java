/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperatorProviderTest {

    @Mock
    Operator<Boolean> testOperator;

    OperatorProvider objectUnderTest;

    @Test
    void testGetExistingOperator() {
        final int testSymbol = 100;
        when(testOperator.getSymbol()).thenReturn(testSymbol);
        final List<Operator<?>> testOperators = Collections.singletonList(testOperator);

        objectUnderTest = new OperatorProvider(testOperators);
        final Operator<?> retrievedOperator =  objectUnderTest.getOperator(testSymbol);
        assertThat(retrievedOperator, notNullValue());
        assertThat(retrievedOperator.getSymbol(), equalTo(testSymbol));
    }

    @Test
    void testGetNonExistingOperator() {
        objectUnderTest = new OperatorProvider(Collections.emptyList());
        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.getOperator(100));
    }
}