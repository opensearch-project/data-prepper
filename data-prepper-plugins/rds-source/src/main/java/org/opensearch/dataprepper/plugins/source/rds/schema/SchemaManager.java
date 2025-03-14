package org.opensearch.dataprepper.plugins.source.rds.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for manager classes that are used to get metadata of a database, such as table schemas
 */
public interface SchemaManager {
    /**
     * Get the primary keys for a table
     * @param fullTableNames A list of full table names
     * @return Map of table name to primary keys
     */
    Map<String, List<String>> getPrimaryKeys(final List<String> fullTableNames);

    /**
     * Get the mapping of columns to data types for a table
     * @param fullTableNames A list of full table names
     * @return Map of table name to it column type map
     */
    Map<String, Map<String,String>> getColumnDataTypes(final List<String> fullTableNames);

    /**
     * Get the list of table names in a database
     * @param databaseName The database name
     * @return Set of table names
     */
    Set<String> getTableNames(final String databaseName);
}
