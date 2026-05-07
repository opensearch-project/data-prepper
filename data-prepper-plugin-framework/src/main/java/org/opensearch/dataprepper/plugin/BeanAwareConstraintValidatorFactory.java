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
import jakarta.validation.Validator;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Gets {@link ConstraintValidator} beans from the application config and makes them
 * available to the Hibernate {@link Validator} as created in {@link ValidatorConfiguration}.
 * <p>
 * This allows us to create {@link ConstraintValidator} implementations that rely on Spring
 * dependency injection.
 */
class BeanAwareConstraintValidatorFactory implements ConstraintValidatorFactory {
    private final ConstraintValidatorFactory delegate;
    private final Map<Class<?>, ConstraintValidator<?, ?>> validatorsByClass;

    BeanAwareConstraintValidatorFactory(final ConstraintValidatorFactory delegate,
                                        final Collection<ConstraintValidator<?, ?>> constraintValidators) {
        this.delegate = delegate;
        this.validatorsByClass = constraintValidators.stream()
                .collect(Collectors.toMap(Object::getClass, Function.identity()));
    }

    @Override
    public <T extends ConstraintValidator<?, ?>> T getInstance(final Class<T> key) {
        final ConstraintValidator<?, ?> validator = validatorsByClass.get(key);
        if (validator != null) {
            return key.cast(validator);
        }
        return delegate.getInstance(key);
    }

    @Override
    public void releaseInstance(final ConstraintValidator<?, ?> instance) {
        if (!validatorsByClass.containsValue(instance)) {
            delegate.releaseInstance(instance);
        }
    }
}
