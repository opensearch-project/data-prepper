/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;

import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface DBSource {

    void initiateDownload(List<DatabasePathURLConfig> config);

    void buildRequestAndDownloadFile(String key);

    /**
     * createFolderIfNotExist
     * @param outputFilePath Output File Path
     * @return File
     */
    static File createFolderIfNotExist(String outputFilePath) {
       //TODO
        return null;
    }

    /**
     * deleteDirectory
     * @param file file
     */
    static void deleteDirectory(File file) {
       //TODO
    }

    /**
     * initiateSSL
     * @throws NoSuchAlgorithmException NoSuchAlgorithmException
     * @throws KeyManagementException KeyManagementException
     */
    default void initiateSSL() throws NoSuchAlgorithmException, KeyManagementException {
        //TODO
    }
}
