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

    private String tempPath;
    private final String prefixDir;

    /**
     * LocalDBDownloadService constructor for initialisation of attributes
     * @param tempPath tempPath
     * @param prefixDir prefixDir
     */
    public LocalDBDownloadService(String tempPath, String prefixDir) {
        this.tempPath = tempPath;
        this.prefixDir = prefixDir;
    }

    /**
     * Initialisation of Download from local file path
     * @param config config
     */
    @Override
    public void initiateDownload(List<String> config) throws Exception {
        String destPath = tempPath + File.separator + prefixDir;
        DBSource.createFolderIfNotExist(destPath);
        File srcDatabaseConfigPath = new File(config.get(0));
        File destDatabaseConfigPath = new File(destPath);
        FileUtils.copyDirectory(srcDatabaseConfigPath, destDatabaseConfigPath);
    }
}
