package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BinaryTypeHandlerTest {

    @Test
    public void testHandleByteArrayData() {
        final DataTypeHandler handler = new BinaryTypeHandler();
        final MySQLDataType columnType = MySQLDataType.BINARY;
        final String columnName = "binaryColumn";
        final String testData = UUID.randomUUID().toString();
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName));
        final Object result = handler.handle(columnType, columnName, testData.getBytes(), metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(testData));
    }

    @Test
    public void testHandleMapWithByteArrayData() {
        final DataTypeHandler handler = new BinaryTypeHandler();
        final MySQLDataType columnType = MySQLDataType.BINARY;
        final String columnName = "test_column";
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName));
        final String testData = UUID.randomUUID().toString();
        final Map<String, Object> value = new HashMap<>();
        value.put("bytes", testData.getBytes());

        final Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(testData));
    }

    @Test
    public void testHandleMapValueNotByteArray() {
        final DataTypeHandler handler = new BinaryTypeHandler();
        final MySQLDataType columnType = MySQLDataType.BINARY;
        final String columnName = "test_column";
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName));
        final Map<String, Object> value = new HashMap<>();
        final String testData = UUID.randomUUID().toString();
        value.put("bytes", testData);

        final Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(testData));
    }

    @Test
    public void testHandleNonByteArrayNonMapValue() {
        final DataTypeHandler handler = new BinaryTypeHandler();
        final MySQLDataType columnType = MySQLDataType.BINARY;
        final String columnName = "test_column";
        final Integer value = 42;
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName));

        final Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is("42"));
    }
}
