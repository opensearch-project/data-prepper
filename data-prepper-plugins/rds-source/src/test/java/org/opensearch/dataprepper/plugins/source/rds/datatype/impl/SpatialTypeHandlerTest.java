package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SpatialTypeHandlerTest {

    @Test
    public void test_handle() {
        final DataTypeHandler handler = new SpatialTypeHandler();
        final MySQLDataType columnType = MySQLDataType.GEOMETRY;
        final String columnName = "geometryColumn";
        final String value = UUID.randomUUID().toString();
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, value.getBytes(), metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }


    @Test
    public void testHandleMapWithByteArrayData() {
        final DataTypeHandler handler = new SpatialTypeHandler();
        final MySQLDataType columnType = MySQLDataType.GEOMETRY;
        final String columnName = "geometryColumn";
        final String testData = UUID.randomUUID().toString();
        final Map<String, Object> value = new HashMap<>();
        value.put("bytes", testData.getBytes());
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(testData));
    }

    @Test
    public void testHandleMapWithByteStringData() {
        final DataTypeHandler handler = new SpatialTypeHandler();
        final MySQLDataType columnType = MySQLDataType.GEOMETRY;
        final String columnName = "geometryColumn";
        final String testData = UUID.randomUUID().toString();
        final Map<String, Object> value = new HashMap<>();
        value.put("bytes", testData);
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(testData));
    }
}
