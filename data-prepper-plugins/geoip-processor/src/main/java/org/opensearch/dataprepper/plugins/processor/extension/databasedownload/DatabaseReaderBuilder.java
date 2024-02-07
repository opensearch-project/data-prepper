/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Implementation class for DatabaseReader Creation
 */
public class DatabaseReaderCreate {
    private DatabaseReaderCreate() {
    }

    /**
     * Creates DatabaseReader instance based on in memory or cache type
     * @param databasePath databasePath
     * @param cacheSize cacheSize
     * @return DatabaseReader
     */
    public static DatabaseReader buildReader(final Path databasePath, final int cacheSize) throws IOException {
        return new DatabaseReader.Builder(databasePath.toFile())
                .fileMode(Reader.FileMode.MEMORY_MAPPED)
                .withCache(new CHMCache(cacheSize))
                .build();
    }
}
