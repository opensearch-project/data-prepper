package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonTypeHandlerTest {

    @Test
    public void testHandleJsonBytes() {
        final DataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final String jsonValue = "{\"key\":\"value\"}";
        final byte[] testData = jsonValue.getBytes();
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, testData, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(jsonValue));
    }

    @Test
    public void testHandleJsonString() {
        final DataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final String jsonValue = "{\"key\":\"value\"}";
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, jsonValue, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(jsonValue));
    }

    @Test
    public void testHandleInvalidJsonBytes() {
        final DataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final byte[] testData = new byte[]{5};
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        assertThrows(RuntimeException.class, () ->  handler.handle(columnType, columnName, testData, metadata));
    }
}
