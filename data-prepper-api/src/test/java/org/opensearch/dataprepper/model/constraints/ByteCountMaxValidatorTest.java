/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.constraints;

import jakarta.validation.ConstraintValidatorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.types.ByteCount;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ByteCountMaxValidatorTest {
    @Mock
    private ByteCountMax byteCountMax;

    @Mock
    private ConstraintValidatorContext context;

    private ByteCountMaxValidator createObjectUnderTest() {
        final ByteCountMaxValidator objectUnderTest = new ByteCountMaxValidator();
        objectUnderTest.initialize(byteCountMax);
        return objectUnderTest;
    }

    @Test
    void isValid_returns_true_for_null() {
        when(byteCountMax.value()).thenReturn("0b");
        assertThat(createObjectUnderTest().isValid(null, context), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
            "1b, 1b",
            "10kb, 1b",
            "10kb, 9kb",
            "10kb, 10kb",
            "10kb, 900b"
    })
    void isValid_returns_true_for_values_less_than_or_equal_to_the_maximum(final String maxBytes, final String givenBytes) {
        when(byteCountMax.value()).thenReturn(maxBytes);
        final ByteCount givenByteCount = ByteCount.parse(givenBytes);
        assertThat(createObjectUnderTest().isValid(givenByteCount, context), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
            "1b, 2b",
            "10kb, 11kb",
            "10kb, 1mb"
    })
    void isValid_returns_false_for_values_greater_than_the_maximum(final String maxBytes, final String givenBytes) {
        when(byteCountMax.value()).thenReturn(maxBytes);
        final ByteCount givenByteCount = ByteCount.parse(givenBytes);

        final ConstraintValidatorContext.ConstraintViolationBuilder violationBuilder =
                mock(ConstraintValidatorContext.ConstraintViolationBuilder.class);
        when(context.buildConstraintViolationWithTemplate(anyString()))
                .thenReturn(violationBuilder);

        assertThat(createObjectUnderTest().isValid(givenByteCount, context), equalTo(false));

        verify(context).disableDefaultConstraintViolation();
        final ArgumentCaptor<String> errorMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(context).buildConstraintViolationWithTemplate(errorMessageCaptor.capture());

        final String errorMessage = errorMessageCaptor.getValue();

        assertThat(errorMessage, containsString(givenByteCount.toString()));
        assertThat(errorMessage, containsString(ByteCount.parse(maxBytes).toString()));
    }

    @ParameterizedTest
    @CsvSource({
            "1b, 2b",
            "10kb, 11kb",
            "10kb, 1mb"
    })
    void isValid_returns_false_for_values_greater_than_the_maximum_when_context_is_null(final String maxBytes, final String givenBytes) {
        when(byteCountMax.value()).thenReturn(maxBytes);
        final ByteCount givenByteCount = ByteCount.parse(givenBytes);

        assertThat(createObjectUnderTest().isValid(givenByteCount, null), equalTo(false));
    }
}