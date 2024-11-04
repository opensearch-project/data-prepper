package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BinaryTypeHandlerTest {

    @Test
    public void test_handle() {
        final DataTypeHandler handler = new BinaryTypeHandler();
        final MySQLDataType columnType = MySQLDataType.BINARY;
        final String columnName = "binaryColumn";
        final byte[] testData = "Test binary data".getBytes();
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        Object result = handler.handle(columnType, columnName, testData, metadata);

        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(Base64.getEncoder().encodeToString(testData)));
    }
}
