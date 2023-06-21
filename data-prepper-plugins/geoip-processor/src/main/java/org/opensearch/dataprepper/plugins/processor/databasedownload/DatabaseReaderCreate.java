/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.processor.databasedownload;

import com.maxmind.geoip2.DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;

import java.nio.file.Path;

/**
 * Implementation class for DatabaseReader Creation
 */
public class DatabaseReaderCreate {

    private static String[] DEFAULT_DATABASE_FILENAMES = new String[] { "GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb" };

    /**
     * Creates DatabaseReader instance based on in memory or cache type
     * @param databasePath databasePath
     * @param loadDatabaseType loadDatabaseType
     * @param cacheSize cacheSize
     * @return DatabaseReader
     */
    public static DatabaseReader.Builder createLoader(Path databasePath, LoadTypeOptions loadDatabaseType, int cacheSize) {

        //TODO:  Create DatabaseReader based on in_memory enum
        //TODO: Create DatabaseReader based on cache enum
        return null;
    }

    /**
     * Creates DatabaseReader instance
     * @param databasePath databasePath
     * @return DatabaseReader instance
     */
    public static DatabaseReader.Builder createDatabaseBuilder(Path databasePath) {
       //TODO: Creates DatabaseReader instance
        return null;
    }
}
