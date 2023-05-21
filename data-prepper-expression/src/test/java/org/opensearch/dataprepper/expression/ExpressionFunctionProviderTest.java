/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.lang3.RandomStringUtils;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;

import java.util.List;
import java.util.function.Function;

@ExtendWith(MockitoExtension.class)
class ExpressionFunctionProviderTest {
    private ExpressionFunctionProvider objectUnderTest;
    private String testFunctionName;
    private Object testResultObject;
    private ExpressionFunction expressionFunction;
    private Function<Object, Object> testFunction;
    private Event testEvent;

    public ExpressionFunctionProvider createObjectUnderTest() {
        expressionFunction = mock(ExpressionFunction.class);
        testFunction = mock(Function.class);
        testFunctionName = RandomStringUtils.randomAlphabetic(8);
        testEvent = mock(Event.class);
        testResultObject = mock(Object.class);
        lenient().when(expressionFunction.evaluate(any(List.class), any(Event.class), any(Function.class))).thenReturn(testResultObject);
        lenient().when(expressionFunction.getFunctionName()).thenReturn(testFunctionName);

        return new ExpressionFunctionProvider(List.of(expressionFunction));
    }

    @Test
    void testUnknownFunction() {
        objectUnderTest = createObjectUnderTest();
        String unknownFunctionName = RandomStringUtils.randomAlphabetic(8);
        assertThrows(RuntimeException.class, () -> objectUnderTest.provideFunction(unknownFunctionName, List.of(), testEvent, testFunction));
    }

    @Test
    void testFunctionBasic() {
        objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.provideFunction(testFunctionName, List.of(), testEvent, testFunction), equalTo(testResultObject));
    }

}
