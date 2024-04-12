/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.validator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsAccountIdConstraintValidatorTest {

    private static AwsAccountIdConstraintValidator createObjectUnderTest() {
        return new AwsAccountIdConstraintValidator();
    }

    @Test
    void isValid_with_null_returns_true() {
        assertThat(createObjectUnderTest().isValid(null, null),
                equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"123456789012", "000011112222", "000000000000"})
    void isValid_with_valid_accountId_returns_true(final String stringValue) {
        assertThat(createObjectUnderTest().isValid(stringValue, null),
                equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "12345678901", "1234567890123", "12345678901b", "a23456789012", "-23456789012"})
    void isValid_with_invalid_accountId_returns_false(final String stringValue) {
        assertThat(createObjectUnderTest().isValid(stringValue, null),
                equalTo(false));
    }

}