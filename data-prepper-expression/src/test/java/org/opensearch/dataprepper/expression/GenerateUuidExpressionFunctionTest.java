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
import org.opensearch.dataprepper.model.event.Event;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class GenerateUuidExpressionFunctionTest {

    private final GenerateUuidExpressionFunction function = new GenerateUuidExpressionFunction();
    private final Event event = mock(Event.class);
    private final Function<Object, Object> convertLiteralType = v -> v;

    @Test
    void getFunctionName_returns_generateUuid() {
        assertThat(function.getFunctionName(), equalTo("generateUuid"));
    }

    @Test
    void evaluate_returns_valid_uuid_string() {
        final Object result = function.evaluate(Collections.emptyList(), event, convertLiteralType);
        assertThat(result, instanceOf(String.class));
        final String uuidStr = (String) result;
        final UUID regeneratedUuid = assertDoesNotThrow(() -> UUID.fromString(uuidStr));
        assertThat(regeneratedUuid.toString(), equalTo(uuidStr));
    }

    @Test
    void evaluate_returns_unique_values_on_successive_calls() {
        final String first = (String) function.evaluate(Collections.emptyList(), event, convertLiteralType);
        final String second = (String) function.evaluate(Collections.emptyList(), event, convertLiteralType);
        assertThat(first, not(equalTo(second)));
    }

    @Test
    void evaluate_throws_when_args_are_provided() {
        assertThrows(RuntimeException.class,
                () -> function.evaluate(List.of("unexpected"), event, convertLiteralType));
    }
}
