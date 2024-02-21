/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;

/**
 * Implementation class for Download through local path
 */
public class LocalDBDownloadService implements DBSource {

    private final String destinationDirectory;

    /**
     * LocalDBDownloadService constructor for initialisation of attributes
     * @param destinationDirectory destinationDirectory
     */
    public LocalDBDownloadService(final String destinationDirectory) {
        this.destinationDirectory = destinationDirectory;
    }

    /**
     * Initialisation of Download from local file path
     * @param config config
     */
    @Override
    public void initiateDownload(List<String> config) throws Exception {
        File srcDatabaseConfigPath = new File(config.get(0));
        File destDatabaseConfigPath = new File(destinationDirectory);
        FileUtils.copyDirectory(srcDatabaseConfigPath, destDatabaseConfigPath);
    }
}
