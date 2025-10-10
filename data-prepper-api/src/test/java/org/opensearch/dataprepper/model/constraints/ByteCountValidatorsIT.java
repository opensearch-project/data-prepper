/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.constraints;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ByteCountValidatorsIT {
    private Validator validator;

    private static class ValidatedClass {
        @ByteCountMin("512b")
        @ByteCountMax("24kb")
        private ByteCount value;
    }

    @BeforeEach
    void setUp() {
        final ValidatorFactory validationFactory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        validator = validationFactory.getValidator();
    }

    @ParameterizedTest
    @ValueSource(strings = {"512b", "1kb", "15kb", "24kb"})
    void validator_has_no_violations_for_valid_values(final String byteString) {
        final ValidatedClass validatedClass = new ValidatedClass();
        validatedClass.value = ByteCount.parse(byteString);

        final Set<ConstraintViolation<ValidatedClass>> violations = validator.validate(validatedClass);

        assertThat(violations, notNullValue());
        assertThat(violations.size(), equalTo(0));
    }

    @ParameterizedTest
    @CsvSource({
            "0b, below minimum",
            "1b, below minimum",
            "511b, below minimum",
            "25kb, exceeds maximum",
            "1mb, exceeds maximum",
            "1gb, exceeds maximum"
    })
    void validator_includes_violations_for_invalid_values(final String byteString, final String expectedMessagePart) {
        final ValidatedClass validatedClass = new ValidatedClass();
        final ByteCount byteCount = ByteCount.parse(byteString);
        validatedClass.value = byteCount;

        final Set<ConstraintViolation<ValidatedClass>> violations = validator.validate(validatedClass);

        assertThat(violations, notNullValue());
        assertThat(violations.size(), equalTo(1));
        final ConstraintViolation<ValidatedClass> violation = violations.stream().findFirst().get();

        assertThat(violation.getMessage(), containsString(byteCount.toString()));
        assertThat(violation.getMessage(), containsString(expectedMessagePart));
    }
}
