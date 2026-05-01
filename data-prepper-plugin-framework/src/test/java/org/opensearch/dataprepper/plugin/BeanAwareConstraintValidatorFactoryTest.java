/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import jakarta.validation.ConstraintValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class BeanAwareConstraintValidatorFactoryTest {
    @Mock
    private ConstraintValidator<?, ?> constraintValidator;

    @Test
    void getInstance_returns_injected_validator_when_class_matches() {
        final BeanAwareConstraintValidatorFactory objectUnderTest =
                new BeanAwareConstraintValidatorFactory(List.of(constraintValidator));

        final ConstraintValidator<?, ?> instance = objectUnderTest.getInstance(constraintValidator.getClass());

        assertThat(instance, sameInstance(constraintValidator));
    }

    @Test
    void getInstance_delegates_to_default_factory_when_class_not_in_injected_list() {
        final BeanAwareConstraintValidatorFactory objectUnderTest =
                new BeanAwareConstraintValidatorFactory(Collections.emptyList());

        final ConstraintValidator<?, ?> instance = objectUnderTest.getInstance(constraintValidator.getClass());

        assertThat(instance, notNullValue());
    }
}
