/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorFactory;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.internal.engine.constraintvalidation.ConstraintValidatorFactoryImpl;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.util.Collection;
import java.util.List;

/**
 * Application context for internal plugin framework beans.
 */
@Named
class ValidatorConfiguration {
    @Bean
    ConstraintValidatorFactory constraintValidatorFactory(final Collection<ConstraintValidator<?, ?>> constraintValidators) {
        return new BeanAwareConstraintValidatorFactory(new ConstraintValidatorFactoryImpl(), constraintValidators);
    }

    @Bean
    Validator validator(final ConstraintValidatorFactory constraintValidatorFactory,
                        final List<ConstraintMappingContributor> constraintMappingContributors) {
        final HibernateValidatorConfiguration configuration = Validation.byProvider(HibernateValidator.class)
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .constraintValidatorFactory(constraintValidatorFactory);

        constraintMappingContributors.forEach(contributor -> contributor.addConstraintMapping(configuration));

        final ValidatorFactory validationFactory = configuration.buildValidatorFactory();
        return validationFactory.getValidator();
    }

    @Bean
    LevenshteinDistance levenshteinDistance() {
        return new LevenshteinDistance();
    }
}
