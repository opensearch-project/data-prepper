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
import jakarta.validation.ConstraintValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class BeanAwareConstraintValidatorFactoryTest {
    @Mock
    private ConstraintValidatorFactory delegate;

    @Mock
    private ConstraintValidator<?, ?> constraintValidator;

    private Collection<ConstraintValidator<?, ?>> constraintValidators;

    @BeforeEach
    void setUp() {
        constraintValidators = Collections.emptyList();
    }

    private BeanAwareConstraintValidatorFactory createObjectUnderTest() {
        return new BeanAwareConstraintValidatorFactory(delegate, constraintValidators);
    }

    @Test
    void getInstance_returns_injected_validator_when_class_matches() {
        constraintValidators = List.of(constraintValidator);

        final ConstraintValidator<?, ?> instance = createObjectUnderTest().getInstance(constraintValidator.getClass());

        assertThat(instance, sameInstance(constraintValidator));
        verifyNoInteractions(delegate);
    }

    @Test
    void getInstance_delegates_to_default_factory_when_class_not_in_injected_list() {
        final ConstraintValidator<?, ?> delegateResult = mock(ConstraintValidator.class);
        doReturn(delegateResult).when(delegate).getInstance(constraintValidator.getClass());

        final ConstraintValidator<?, ?> instance = createObjectUnderTest().getInstance(constraintValidator.getClass());

        assertThat(instance, sameInstance(delegateResult));
    }

    @Test
    void releaseInstance_does_not_delegate_when_instance_is_injected() {
        constraintValidators = List.of(constraintValidator);

        createObjectUnderTest().releaseInstance(constraintValidator);

        verifyNoInteractions(delegate);
    }

    @Test
    void releaseInstance_delegates_to_default_factory_when_instance_is_not_injected() {
        createObjectUnderTest().releaseInstance(constraintValidator);

        verify(delegate).releaseInstance(constraintValidator);
    }
}
