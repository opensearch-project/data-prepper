package org.opensearch.dataprepper.plugins.source.rds.schema;

import java.util.List;
import java.util.Map;

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
    /**
     * Get the mapping of columns to data types for a table
     * @param fullTableName The full table name
     * @return Map of column names to data types
     */
    Map<String,String> getColumnDataTypes(final String fullTableName);
}
