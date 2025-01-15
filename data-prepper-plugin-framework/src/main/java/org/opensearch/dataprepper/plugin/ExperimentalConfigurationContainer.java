/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

/**
 * Interface to decouple how an experimental configuration is defined from
 * usage of those configurations.
 *
 * @since 2.11
 */
public interface ExperimentalConfigurationContainer {
    /**
     * Gets the experimental configuration.
     * @return the experimental configuration
     * @since 2.11
     */
    ExperimentalConfiguration getExperimental();
}
