/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.hibernate.validator.internal.engine.constraintvalidation.ConstraintValidatorFactoryImpl;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.Experimental;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ValidatorConfigurationTest {

    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;

    @Mock
    private ExperimentalConfiguration experimentalConfiguration;

    private Validator createValidator() {
        when(experimentalConfigurationContainer.getExperimental()).thenReturn(experimentalConfiguration);
        final ExperimentalFeatureValidator experimentalFeatureValidator =
                new ExperimentalFeatureValidator(experimentalConfigurationContainer);
        final BeanAwareConstraintValidatorFactory constraintValidatorFactory =
                new BeanAwareConstraintValidatorFactory(new ConstraintValidatorFactoryImpl(), List.of(experimentalFeatureValidator));
        final ExperimentalConstraintMappingContributor experimentalContributor =
                new ExperimentalConstraintMappingContributor();
        return new ValidatorConfiguration().validator(
                constraintValidatorFactory, List.of(experimentalContributor));
    }

    static class ConfigWithExperimentalField {
        @Experimental
        private Object experimentalFeature;

        ConfigWithExperimentalField(final Object experimentalFeature) {
            this.experimentalFeature = experimentalFeature;
        }
    }

    @Nested
    class ExperimentalFieldValidation {
        @Test
        void validator_allows_null_experimental_field_when_experimental_is_not_enabled() {
            final Validator validator = createValidator();

            final Set<ConstraintViolation<ConfigWithExperimentalField>> violations =
                    validator.validate(new ConfigWithExperimentalField(null));

            assertThat(violations, empty());
        }

        @Test
        void validator_rejects_non_null_experimental_field_when_experimental_is_not_enabled() {
            final Validator validator = createValidator();

            final Set<ConstraintViolation<ConfigWithExperimentalField>> violations =
                    validator.validate(new ConfigWithExperimentalField(new Object()));

            assertThat(violations, hasSize(1));
            final ConstraintViolation<ConfigWithExperimentalField> violation = violations.iterator().next();
            assertThat(violation.getPropertyPath().toString(), equalTo("experimentalFeature"));
        }

        @Test
        void validator_allows_non_null_experimental_field_when_enableAll_is_true() {
            when(experimentalConfiguration.isEnableAll()).thenReturn(true);
            final Validator validator = createValidator();

            final Set<ConstraintViolation<ConfigWithExperimentalField>> violations =
                    validator.validate(new ConfigWithExperimentalField(new Object()));

            assertThat(violations, empty());
        }
    }
}
