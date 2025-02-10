package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHandler;
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
        final MySQLDataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final String jsonValue = "{\"key\":\"value\"}";
        final byte[] testData = jsonValue.getBytes();
        final TableMetadata metadata = TableMetadata.builder()
                .withTableName(UUID.randomUUID().toString())
                .withDatabaseName(UUID.randomUUID().toString())
                .withColumnNames(List.of(columnName))
                .withPrimaryKeys(List.of(columnName))
                .withEnumStrValues(Collections.emptyMap())
                .withSetStrValues(Collections.emptyMap())
                .build();
        Object result = handler.handle(columnType, columnName, testData, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(jsonValue));
    }

    @Test
    public void testHandleJsonString() {
        final MySQLDataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final String jsonValue = "{\"key\":\"value\"}";
        final TableMetadata metadata = TableMetadata.builder()
                .withTableName(UUID.randomUUID().toString())
                .withDatabaseName(UUID.randomUUID().toString())
                .withColumnNames(List.of(columnName))
                .withPrimaryKeys(List.of(columnName))
                .withEnumStrValues(Collections.emptyMap())
                .withSetStrValues(Collections.emptyMap())
                .build();
        Object result = handler.handle(columnType, columnName, jsonValue, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(jsonValue));
    }

    @Test
    public void testHandleInvalidJsonBytes() {
        final MySQLDataTypeHandler handler = new JsonTypeHandler();
        final MySQLDataType columnType = MySQLDataType.JSON;
        final String columnName = "jsonColumn";
        final byte[] testData = new byte[]{5};
        final TableMetadata metadata = TableMetadata.builder()
                .withTableName(UUID.randomUUID().toString())
                .withDatabaseName(UUID.randomUUID().toString())
                .withColumnNames(List.of(columnName))
                .withPrimaryKeys(List.of(columnName))
                .withEnumStrValues(Collections.emptyMap())
                .withSetStrValues(Collections.emptyMap())
                .build();
        assertThrows(RuntimeException.class, () ->  handler.handle(columnType, columnName, testData, metadata));
    }
}
