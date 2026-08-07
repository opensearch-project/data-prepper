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
 * Model that should be provided to plugins to provide certain details about the pipeline they belong to
 * @since 1.3
 */
public interface PipelineDescription {
    /**
     * Returns the name of the pipeline that a plugin belongs to
     * 
     * @return returns pipeline name
     * @since 1.3
     */
    String getPipelineName();

    /**
     * Returns the number of process workers the pipeline is using; plugins can utilize this if synchronization between workers is necessary
     *
     * @return returns the number of process workers
     * @since 1.3
     */
    int getNumberOfProcessWorkers();
}
