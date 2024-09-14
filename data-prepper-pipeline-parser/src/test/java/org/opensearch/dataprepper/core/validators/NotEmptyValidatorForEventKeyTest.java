/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.validators;

import jakarta.validation.ConstraintValidatorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotEmptyValidatorForEventKeyTest {
    @Mock
    private EventKey eventKey;

    @Mock
    private ConstraintValidatorContext context;

    private NotEmptyValidatorForEventKey createObjectUnderTest() {
        return new NotEmptyValidatorForEventKey();
    }

    @Test
    void isValid_returns_false_if_EventKey_is_empty() {
        assertThat(createObjectUnderTest().isValid(null, context), equalTo(false));
    }

    @Test
    void isValid_returns_false_if_EventKey_getKey_is_empty() {
        when(eventKey.getKey()).thenReturn("");
        assertThat(createObjectUnderTest().isValid(eventKey, context), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "a", "/abcdefghijklmnopqrstuvwxyz"})
    void isValid_returns_true_if_EventKey_getKey_is_not_empty(final String key) {
        when(eventKey.getKey()).thenReturn(key);
        assertThat(createObjectUnderTest().isValid(eventKey, context), equalTo(true));
    }
}