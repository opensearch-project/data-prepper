/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ToJsonStringExpressionFunctionTest {

    @Mock
    private Event event;

    @Mock
    private Function<Object, Object> convertLiteralType;

    private ToJsonStringExpressionFunction createObjectUnderTest() {
        return new ToJsonStringExpressionFunction();
    }

    @Test
    void getFunctionName_returns_functionName() {
        assertThat(createObjectUnderTest().getFunctionName(), equalTo("toJsonString"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void evaluate_throws_if_arguments_has_size_greater_than_one(final int numberOfArguments) {
        final List<Object> arguments = IntStream.range(0, numberOfArguments)
                .mapToObj(i -> "arg" + i)
                .collect(Collectors.toList());

        final ToJsonStringExpressionFunction objectUnderTest = createObjectUnderTest();

        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(arguments, event, convertLiteralType));
    }

    @Test
    void evaluate_returns_Event_toJsonString() {
        final List<Object> arguments = Collections.emptyList();

        final String jsonString = UUID.randomUUID().toString();
        when(event.toJsonString()).thenReturn(jsonString);

        assertThat(createObjectUnderTest().evaluate(arguments, event, convertLiteralType),
                equalTo(jsonString));
    }
}