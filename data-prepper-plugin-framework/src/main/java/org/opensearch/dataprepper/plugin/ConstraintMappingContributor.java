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

/**
 * This interface allows implementors to add a {@link ConstraintMapping} to the
 * {@link HibernateValidatorConfiguration} that Data Prepper uses to validate plugins.
 */
@FunctionalInterface
interface ConstraintMappingContributor {
    void addConstraintMapping(HibernateValidatorConfiguration configuration);
}
