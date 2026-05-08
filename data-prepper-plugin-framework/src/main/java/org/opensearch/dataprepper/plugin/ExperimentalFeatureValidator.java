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
import jakarta.validation.ConstraintValidatorContext;
import org.opensearch.dataprepper.model.annotations.Experimental;

import javax.inject.Named;

@Named
class ExperimentalFeatureValidator implements ConstraintValidator<Experimental, Object> {
    private final ExperimentalConfiguration experimentalConfiguration;

    ExperimentalFeatureValidator(final ExperimentalConfigurationContainer experimentalConfigurationContainer) {
        this.experimentalConfiguration = experimentalConfigurationContainer.getExperimental();
    }

    @Override
    public boolean isValid(final Object value, final ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        return experimentalConfiguration.isEnableAll();
    }
}
