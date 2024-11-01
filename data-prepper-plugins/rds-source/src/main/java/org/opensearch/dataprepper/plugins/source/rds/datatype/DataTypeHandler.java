package org.opensearch.dataprepper.plugins.source.rds.datatype;

import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

/**
 * Interface for handling MySQL data type conversions.
 * Implementations of this interface are responsible for converting MySQL column values
 * to appropriate string representations based on their data types.
 */
public interface DataTypeHandler {
    /**
     * Handles the conversion of a MySQL column value to its string representation.
     *
     * @param columnType The MySQL data type of the column being processed
     * @param columnName The name of the column being processed
     * @param value The value to be converted, can be null
     * @param metadata Additional metadata about the table structure and properties
     * @return A string representation of the converted value
     */
    String handle(MySQLDataType columnType, String columnName, Object value, TableMetadata metadata);
}
