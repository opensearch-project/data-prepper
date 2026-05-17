/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.cfg.ConstraintMapping;
import org.opensearch.dataprepper.model.annotations.Experimental;

import javax.inject.Named;

@Named
class ExperimentalConstraintMappingContributor implements ConstraintMappingContributor {
    @Override
    public void addConstraintMapping(final HibernateValidatorConfiguration configuration) {
        final ConstraintMapping constraintMapping = configuration.createConstraintMapping();
        constraintMapping.constraintDefinition(Experimental.class)
                .includeExistingValidators(false)
                .validatedBy(ExperimentalFeatureValidator.class);
        configuration.addMapping(constraintMapping);
    }
}
