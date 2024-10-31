package org.opensearch.dataprepper.plugins.source.rds.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class DbMetadataTest {

    @Test
    public void test_fromMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final Map<String, Object> map = new HashMap<>();
        map.put("dbIdentifier", dbIdentifier);
        map.put("hostName", hostName);
        map.put("port", port);

        final DbMetadata result = DbMetadata.fromMap(map);

        assertThat(result.getDbIdentifier(), is(dbIdentifier));
        assertThat(result.getHostName(), is(hostName));
        assertThat(result.getPort(), is(port));
    }

    @Test
    public void test_toMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final DbMetadata dbMetadata = new DbMetadata(dbIdentifier, hostName, port);

        final Map<String, Object> result = dbMetadata.toMap();

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(3));
        assertThat(result.get("dbIdentifier"), is(dbIdentifier));
        assertThat(result.get("hostName"), is(hostName));
        assertThat(result.get("port"), is(port));
    }
}
