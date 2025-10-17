/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

/**
 * An interface for providing the current Data Prepper version
 * as a Java SPI. This is intended only to be used by Data Prepper
 * core or within tests.
 *
 * @since 2.13
 */
public interface VersionProvider {
    /**
     * Gets the current Data Prepper version as a string.
     * @return The Data Prepper version.
     */
    String getVersionString();
}
