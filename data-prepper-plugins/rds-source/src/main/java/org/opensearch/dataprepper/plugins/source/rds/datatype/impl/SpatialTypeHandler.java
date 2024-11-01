package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

public class SpatialTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        // Geometry types are typically returned as WKB (Well-Known Binary)
        // Convert to WKT (Well-Known Text) or handle according to your needs
        //return Base64.getEncoder().encodeToString((byte[]) value);
        //return value.toString();
        return new String((byte[]) value);
    }
}
