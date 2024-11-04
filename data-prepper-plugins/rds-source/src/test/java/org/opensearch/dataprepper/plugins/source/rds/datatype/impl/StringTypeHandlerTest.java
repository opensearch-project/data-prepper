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
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringTypeHandlerTest {
    @Test
    public void test_handle_char_string() {
        DataTypeHandler handler = new StringTypeHandler();
        String columnName = "testColumn";
        MySQLDataType columnType = MySQLDataType.VARCHAR;
        final String value = "Hello, World!";
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());

        Object result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }

    @Test
    public void test_handle_byte_string() {
        DataTypeHandler handler = new StringTypeHandler();
        String columnName = "testColumn";
        MySQLDataType columnType = MySQLDataType.TEXT;
        final String value = "Hello, World!";
        byte[] testBytes = value.getBytes();
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());

        Object result = handler.handle(columnType, columnName, testBytes, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }

    @Test
    public void test_handle_enum_string() {
        StringTypeHandler handler = new StringTypeHandler();
        String columnName = "testColumn";
        Integer value = 2;
        String[] enumValues = { "ENUM1", "ENUM2", "ENUM3" };
        MySQLDataType columnType = MySQLDataType.ENUM;
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Map.of(columnName, enumValues));

        String result = handler.handle(columnType, columnName, value, metadata);

        assertThat(result, is("ENUM2"));
    }

    @Test
    public void test_handle_set_string() {
        StringTypeHandler handler = new StringTypeHandler();
        String columnName = "testColumn";
        Long value = 3L;
        String[] setStrValues = { "Value1", "Value2", "Value3" };
        Map<String, String[]> setStrValuesMap = new HashMap<>();
        setStrValuesMap.put(columnName, setStrValues);
        MySQLDataType columnType = MySQLDataType.SET;
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                setStrValuesMap, Collections.emptyMap());

        String result = handler.handle(columnType, columnName, value, metadata);

        assertEquals("[Value1, Value2]", result);
    }
}
