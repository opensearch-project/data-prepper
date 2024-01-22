/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;

import java.nio.file.Path;

/**
 * Implementation class for DatabaseReader Creation
 */
public class DatabaseReaderCreate {

    /**
     * Creates DatabaseReader instance based on in memory or cache type
     * @param databasePath databasePath
     * @param cacheSize cacheSize
     * @return DatabaseReader
     */
    public static DatabaseReader.Builder createLoader(final Path databasePath, final int cacheSize) {

        return new DatabaseReader.Builder(databasePath.toFile())
                .fileMode(Reader.FileMode.MEMORY_MAPPED)
                .withCache(new CHMCache(cacheSize));
    }
}
