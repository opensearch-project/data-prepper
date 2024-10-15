/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import javax.annotation.Nullable;

/**
 * Provides information on the file structure for Data Prepper that can be used
 * within Data Prepper core to locate necessary files.
 *
 * @since 2.1
 */
public interface FileStructurePathProvider {
    /**
     * Gets the location of the pipeline configuration file or
     * pipeline configuration directory.
     *
     * @return The path to the pipeline file or directory.
     */
    String getPipelineConfigFileLocation();

    /**
     * Gets the location of the Data Prepper configuration file.
     *
     * @return The path to the Data Prepper configuration file.
     */
    @Nullable
    String getDataPrepperConfigFileLocation();
}
