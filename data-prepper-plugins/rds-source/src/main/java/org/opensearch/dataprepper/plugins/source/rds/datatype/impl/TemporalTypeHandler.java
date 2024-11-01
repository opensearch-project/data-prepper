package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class TemporalTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        // Date and Time types
        switch (columnType) {
            // Date and Time types
            case DATE:
                return value instanceof LocalDate ?
                        ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE) :
                        value.toString();

            case TIME:
                return value instanceof LocalTime ?
                        ((LocalTime) value).format(DateTimeFormatter.ISO_LOCAL_TIME) :
                        value.toString();

            case TIMESTAMP:
            case DATETIME:
                /*if (value instanceof Timestamp) {
                    return ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                } else if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }*/
                return value.toString();

            case YEAR:
                return value.toString();
            default:
                throw new IllegalArgumentException("Unsupported temporal data type: " + columnType);
        }

    }
}
