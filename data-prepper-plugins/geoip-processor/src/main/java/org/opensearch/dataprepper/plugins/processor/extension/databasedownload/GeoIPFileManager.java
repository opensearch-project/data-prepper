/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import java.io.File;

public class GeoIPFileManager {
    public void deleteDirectory(final File file) {

        if (file.exists()) {
            for (final File subFile : file.listFiles()) {
                if (subFile.isDirectory()) {
                    deleteDirectory(subFile);
                }
                subFile.delete();
            }
            file.delete();
        }
    }

    public void deleteFile(final File file) {
        file.delete();
    }

    public void createDirectoryIfNotExist(final String outputFilePath) {
        final File destFile = new File(outputFilePath);
        if (!destFile.exists()) {
            destFile.mkdirs();
        }
    }

}
