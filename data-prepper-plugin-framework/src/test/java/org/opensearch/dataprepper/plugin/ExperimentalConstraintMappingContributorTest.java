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
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.internal.engine.constraintvalidation.ConstraintValidatorFactoryImpl;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
class ExperimentalConstraintMappingContributorTest {

    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;

    @Mock
    private ExperimentalConfiguration experimentalConfiguration;

    static class ConfigWithExperimentalField {
        @Experimental
        private Object experimentalFeature;

        ConfigWithExperimentalField(final Object experimentalFeature) {
            this.experimentalFeature = experimentalFeature;
        }
    }

    private Validator createValidator() {
        when(experimentalConfigurationContainer.getExperimental()).thenReturn(experimentalConfiguration);

        final ExperimentalFeatureValidator experimentalFeatureValidator =
                new ExperimentalFeatureValidator(experimentalConfigurationContainer);
        final BeanAwareConstraintValidatorFactory constraintValidatorFactory =
                new BeanAwareConstraintValidatorFactory(new ConstraintValidatorFactoryImpl(), List.of(experimentalFeatureValidator));

        final HibernateValidatorConfiguration configuration = Validation.byProvider(HibernateValidator.class)
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .constraintValidatorFactory(constraintValidatorFactory);

        new ExperimentalConstraintMappingContributor().addConstraintMapping(configuration);

        final ValidatorFactory validatorFactory = configuration.buildValidatorFactory();
        return validatorFactory.getValidator();
    }

    @Test
    void addConstraintMapping_registers_experimental_validation_that_rejects_non_null_field() {
        final Validator validator = createValidator();

        final Set<ConstraintViolation<ConfigWithExperimentalField>> violations =
                validator.validate(new ConfigWithExperimentalField(new Object()));

        assertThat(violations, hasSize(1));
        final ConstraintViolation<ConfigWithExperimentalField> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString(), equalTo("experimentalFeature"));
        assertThat(violation.getMessage(), equalTo("This feature is experimental. You must enable experimental features in data-prepper-config.yaml in order to use them."));
    }

    @Test
    void addConstraintMapping_registers_experimental_validation_that_allows_null_field() {
        final Validator validator = createValidator();

        final Set<ConstraintViolation<ConfigWithExperimentalField>> violations =
                validator.validate(new ConfigWithExperimentalField(null));

        assertThat(violations, empty());
    }
}
