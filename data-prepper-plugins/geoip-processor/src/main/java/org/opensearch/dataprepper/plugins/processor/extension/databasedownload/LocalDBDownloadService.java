/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.google.common.io.Files;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig;

import java.io.File;
import java.util.Set;

/**
 * Implementation class for Download through local path
 */
public class LocalDBDownloadService implements DBSource {

    private final String destinationDirectory;
    private final MaxMindDatabaseConfig maxMindDatabaseConfig;

    /**
     * LocalDBDownloadService constructor for initialisation of attributes
     * @param destinationDirectory destinationDirectory
     */
    public LocalDBDownloadService(final String destinationDirectory, final MaxMindDatabaseConfig maxMindDatabaseConfig) {
        this.destinationDirectory = destinationDirectory;
        this.maxMindDatabaseConfig = maxMindDatabaseConfig;
    }

    /**
     * Initialisation of Download from local file path
     */
    @Override
    public void initiateDownload() throws Exception {
        final Set<String> strings = maxMindDatabaseConfig.getDatabasePaths().keySet();
        for (final String key: strings) {
            Files.copy(new File(maxMindDatabaseConfig.getDatabasePaths().get(key)),
                    new File(destinationDirectory + File.separator + key + MAXMIND_DATABASE_EXTENSION));
        }
    }
}
