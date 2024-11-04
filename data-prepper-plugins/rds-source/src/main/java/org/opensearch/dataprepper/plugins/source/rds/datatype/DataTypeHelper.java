package org.opensearch.dataprepper.plugins.source.rds.datatype;

import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.BinaryTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.JsonTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.NumericTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.SpatialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.StringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.impl.TemporalTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.util.Map;

public class DataTypeHelper {
    private static final Map<MySQLDataType.DataCategory, DataTypeHandler> typeHandlers = Map.of(
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
