package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BinaryTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BitStringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BooleanTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.JsonTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.NetworkAddressTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.NumericTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.SpatialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.SpecialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.StringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.TemporalTypeHandler;


import java.util.Map;

public class PostgresDataTypeHelper {
    private static final Map<PostgresDataType.DataCategory, PostgresDataTypeHandler> typeHandlers = Map.of(
            PostgresDataType.DataCategory.NUMERIC, new NumericTypeHandler(),
            PostgresDataType.DataCategory.STRING, new StringTypeHandler(),
            PostgresDataType.DataCategory.BIT_STRING, new BitStringTypeHandler(),
            PostgresDataType.DataCategory.JSON, new JsonTypeHandler(),
            PostgresDataType.DataCategory.BOOLEAN, new BooleanTypeHandler(),
            PostgresDataType.DataCategory.TEMPORAL, new TemporalTypeHandler(),
            PostgresDataType.DataCategory.SPATIAL, new SpatialTypeHandler(),
            PostgresDataType.DataCategory.NETWORK_ADDRESS, new NetworkAddressTypeHandler(),
            PostgresDataType.DataCategory.SPECIAL, new SpecialTypeHandler(),
            PostgresDataType.DataCategory.BINARY, new BinaryTypeHandler()
    );

    public static Object getDataByColumnType(final PostgresDataType columnType, final String columnName, final Object value
                                             ) {
        if (value == null) {
            return null;
        }

        return typeHandlers.get(columnType.getCategory()).handle(columnType, columnName, value);
    }
}
