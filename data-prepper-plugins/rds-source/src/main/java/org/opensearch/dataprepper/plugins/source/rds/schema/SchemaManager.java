package org.opensearch.dataprepper.plugins.source.rds.schema;

import java.util.List;

/**
 * Interface for manager classes that are used to get metadata of a database, such as table schemas
 */
public interface SchemaManager {
    /**
     * Get the primary keys for a table
     * @param fullTableName The full table name
     * @return List of primary keys
     */
    List<String> getPrimaryKeys(final String fullTableName);
}
