/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.validator;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class AwsAccountIdTest {
    private Validator validator;

    @BeforeEach
    void setUp() {
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
    }

    @Test
    void validate_works_with_actual_Validator_with_valid_accountId() {
        final ValidatedClass validatedClass = new ValidatedClass();
        validatedClass.accountId = "123456789012";

        final Set<ConstraintViolation<ValidatedClass>> violations = validator.validate(validatedClass);

        assertThat(violations, notNullValue());
        assertThat(violations.size(), equalTo(0));
    }

    @Test
    void validate_works_with_actual_Validator_with_invalid_accountId() {
        final ValidatedClass validatedClass = new ValidatedClass();
        validatedClass.accountId = "1234567890ab";

        final Set<ConstraintViolation<ValidatedClass>> violations = validator.validate(validatedClass);

        assertThat(violations, notNullValue());
        assertThat(violations.size(), equalTo(1));
        ConstraintViolation<ValidatedClass> actualViolation = violations.iterator().next();
        assertThat(actualViolation.getMessage(), containsString("AWS account Id"));
        assertThat(actualViolation.getMessage(), containsString("12 digits"));
    }

    private static class ValidatedClass {
        @AwsAccountId
        private String accountId;
    }
}
