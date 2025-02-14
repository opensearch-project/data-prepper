package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql;


import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.BinaryTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.JsonTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.NumericTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.SpatialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.StringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler.TemporalTypeHandler;

import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.util.Map;

public class MySQLDataTypeHelper {
    private static final Map<MySQLDataType.DataCategory, MySQLDataTypeHandler> typeHandlers = Map.of(
            MySQLDataType.DataCategory.NUMERIC, new NumericTypeHandler(),
            MySQLDataType.DataCategory.STRING, new StringTypeHandler(),
            MySQLDataType.DataCategory.TEMPORAL, new TemporalTypeHandler(),
            MySQLDataType.DataCategory.BINARY, new BinaryTypeHandler(),
            MySQLDataType.DataCategory.JSON, new JsonTypeHandler(),
            MySQLDataType.DataCategory.SPATIAL, new SpatialTypeHandler()
    );

    public static Object getDataByColumnType(final MySQLDataType columnType, final String columnName, final Object value,
                                             final TableMetadata metadata) {
        if (value == null) {
            return null;
        }

        return typeHandlers.get(columnType.getCategory()).handle(columnType, columnName, value, metadata);
    }
}