/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;

/**
 * Application context for internal plugin framework beans.
 */
@Named("PluginConfigurationValidationConfiguration")
class PluginConfigurationValidationConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(PluginConfigurationValidationConfiguration.class);

    public PluginConfigurationValidationConfiguration() {
        LOG.error("PluginConfigurationValidationConfiguration bean created");
    }

    @Bean
    Validator validator() {
        final ValidatorFactory validationFactory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        return validationFactory.getValidator();
    }
}
