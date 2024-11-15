package org.opensearch.dataprepper.plugins.source.rds.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata.DB_IDENTIFIER_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata.ENDPOINT_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata.PORT_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata.READER_ENDPOINT_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata.READER_PORT_KEY;

public class DbMetadataTest {

    @Test
    public void test_fromMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String endpoint = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final String readerEndpoint = UUID.randomUUID().toString();
        final Map<String, Object> map = new HashMap<>();
        map.put(DB_IDENTIFIER_KEY, dbIdentifier);
        map.put(ENDPOINT_KEY, endpoint);
        map.put(PORT_KEY, port);
        map.put(READER_ENDPOINT_KEY, readerEndpoint);
        map.put(READER_PORT_KEY, port);

        final DbMetadata result = DbMetadata.fromMap(map);

        assertThat(result.getDbIdentifier(), is(dbIdentifier));
        assertThat(result.getEndpoint(), is(endpoint));
        assertThat(result.getPort(), is(port));
        assertThat(result.getReaderEndpoint(), is(readerEndpoint));
        assertThat(result.getReaderPort(), is(port));
    }

    @Test
    public void test_toMap_success() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String endpoint = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final String readerEndpoint = UUID.randomUUID().toString();
        final DbMetadata dbMetadata = DbMetadata.builder()
                .dbIdentifier(dbIdentifier)
                .endpoint(endpoint)
                .port(port)
                .readerEndpoint(readerEndpoint)
                .readerPort(port)
                .build();

        final Map<String, Object> result = dbMetadata.toMap();

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(5));
        assertThat(result.get(DB_IDENTIFIER_KEY), is(dbIdentifier));
        assertThat(result.get(ENDPOINT_KEY), is(endpoint));
        assertThat(result.get(PORT_KEY), is(port));
        assertThat(result.get(READER_ENDPOINT_KEY), is(readerEndpoint));
        assertThat(result.get(READER_PORT_KEY), is(port));
    }
}
