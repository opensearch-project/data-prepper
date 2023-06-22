/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.processor.databasedownload;

import com.maxmind.db.CHMCache;
import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Implementation class for DatabaseReader Creation
 */
public class DatabaseReaderCreate {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseReaderCreate.class);

    /**
     * Creates DatabaseReader instance based on in memory or cache type
     * @param databasePath databasePath
     * @param loadDatabaseType loadDatabaseType
     * @param cacheSize cacheSize
     * @return DatabaseReader
     */
    public static DatabaseReader.Builder createLoader(Path databasePath, LoadTypeOptions loadDatabaseType, int cacheSize) {

        DatabaseReader.Builder builder = null;

        switch (loadDatabaseType) {
            case INMEMORY:
                builder = createDatabaseBuilder(databasePath).withCache(NoCache.getInstance());
                builder.fileMode(Reader.FileMode.MEMORY_MAPPED);
                break;
            case CACHE:
                builder = createDatabaseBuilder(databasePath).withCache(new CHMCache(cacheSize));
                break;
        }
        return builder;
    }

    /**
     * Creates DatabaseReader instance
     * @param databasePath databasePath
     * @return DatabaseReader instance
     */
    public static DatabaseReader.Builder createDatabaseBuilder(Path databasePath) {
        LOG.info("DatabaseReader Created");
        return new DatabaseReader.Builder(databasePath.toFile());
    }
}
