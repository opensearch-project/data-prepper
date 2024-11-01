package org.opensearch.dataprepper.plugins.source.rds.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DbTableMetadataTest {

    @Test
    public void test_fromMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final String tableName = UUID.randomUUID().toString();

        final DbMetadata dbMetadata = new DbMetadata(dbIdentifier, hostName, port);
        final Map<String, Map<String, String>> tableColumnDataTypeMap = new HashMap<>();
        final Map<String, String> columnDataTypeMap = new HashMap<>();
        columnDataTypeMap.put("int_column", "INTEGER");
        tableColumnDataTypeMap.put(tableName, columnDataTypeMap);


        final Map<String, Object> map = new HashMap<>();
        map.put("dbMetadata", dbMetadata.toMap());
        map.put("tableColumnDataTypeMap", tableColumnDataTypeMap);

        final DbTableMetadata result = DbTableMetadata.fromMap(map);

        assertThat(result.getDbMetadata().getDbIdentifier(), is(dbIdentifier));
        assertThat(result.getDbMetadata().getHostName(), is(hostName));
        assertThat(result.getDbMetadata().getPort(), is(port));
        assertThat(result.getTableColumnDataTypeMap(), is(tableColumnDataTypeMap));
    }

    @Test
    public void test_toMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final String tableName = UUID.randomUUID().toString();

        final DbMetadata dbMetadata = new DbMetadata(dbIdentifier, hostName, port);
        final Map<String, Map<String, String>> tableColumnDataTypeMap = new HashMap<>();
        final Map<String, String> columnDataTypeMap = new HashMap<>();
        columnDataTypeMap.put("int_column", "INTEGER");
        tableColumnDataTypeMap.put(tableName, columnDataTypeMap);
        final DbTableMetadata dbTableMetadata = new DbTableMetadata(dbMetadata, tableColumnDataTypeMap);

        final Map<String, Object> result = dbTableMetadata.toMap();

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(2));
        assertThat(result.get("dbMetadata"), is(dbMetadata.toMap()));
        assertThat(result.get("tableColumnDataTypeMap"), is(tableColumnDataTypeMap));
    }
}
