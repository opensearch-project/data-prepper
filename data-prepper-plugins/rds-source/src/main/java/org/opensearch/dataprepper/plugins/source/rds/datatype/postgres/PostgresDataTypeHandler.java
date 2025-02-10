package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

/**
 * Interface for handling Postgres data type conversions.
 * Implementations of this interface are responsible for converting Postgres column values
 * to appropriate object representations based on their data types.
 */
public interface PostgresDataTypeHandler {
    default Object process(final PostgresDataType columnType, final String columnName, final Object value) {
        if(value == null)
            return null;
        return handle(columnType, columnName, value);
    }
    /**
     * Handles the conversion of a Postgres column value to its object representation.
     *
     * @param columnType The Postgres data type of the column being processed
     * @param columnName The name of the column being processed
     * @param value The value to be converted, can be null
     * @return An object representation of the converted value
     */
    Object handle(PostgresDataType columnType, String columnName, Object value);
}
