package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BinaryTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BitStringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.BooleanTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.JsonTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.NetworkAddressTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.NumericTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.RangeTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.SpatialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.SpecialTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.StringTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler.TemporalTypeHandler;


import java.util.Map;

public class PostgresDataTypeHelper {
    private static final NumericTypeHandler numericTypeHandler = new NumericTypeHandler();
    private static final TemporalTypeHandler temporalTypeHandler = new TemporalTypeHandler();

    private static final Map<PostgresDataType.DataCategory, PostgresDataTypeHandler> typeHandlers = Map.ofEntries(
            Map.entry(PostgresDataType.DataCategory.NUMERIC, numericTypeHandler),
            Map.entry(PostgresDataType.DataCategory.STRING, new StringTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.BIT_STRING, new BitStringTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.JSON, new JsonTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.BOOLEAN, new BooleanTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.TEMPORAL, temporalTypeHandler),
            Map.entry(PostgresDataType.DataCategory.SPATIAL, new SpatialTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.NETWORK_ADDRESS, new NetworkAddressTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.SPECIAL, new SpecialTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.BINARY, new BinaryTypeHandler()),
            Map.entry(PostgresDataType.DataCategory.RANGE, new RangeTypeHandler(numericTypeHandler, temporalTypeHandler))
    );

    public static Object getDataByColumnType(final PostgresDataType columnType, final String columnName, final Object value
                                             ) {
        if (value == null) {
            return null;
        }

        return typeHandlers.get(columnType.getCategory()).handle(columnType, columnName, value);
    }
}
