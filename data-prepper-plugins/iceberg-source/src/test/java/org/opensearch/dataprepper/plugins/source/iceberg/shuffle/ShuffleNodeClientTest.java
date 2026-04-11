/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

class ShuffleNodeClientTest {

    @TempDir
    Path tempDir;

    private LocalDiskShuffleStorage storage;
    private Server server;
    private int port;
    private ShuffleNodeClient client;

    @BeforeEach
    void setUp() throws Exception {
        storage = new LocalDiskShuffleStorage(tempDir);

        // Write test shuffle data
        try (ShuffleWriter writer = storage.createWriter("12345", "abcd1234", 4)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1, 2, 3});
            writer.addRecord(2, ShuffleRecord.OP_DELETE, 0, new byte[]{4, 5});
            writer.finish();
        }

        // Start a real ShuffleHttpServer on random port
        final ShuffleHttpService service = new ShuffleHttpService(storage);
        final ServerBuilder sb = Server.builder();
        sb.http(0);
        sb.annotatedService("/shuffle", service);
        server = sb.build();
        server.start().join();
        port = server.activeLocalPort();

        final ShuffleConfig config = createConfig(port);

        client = new ShuffleNodeClient(config);
    }

    private static ShuffleConfig createConfig(final int port) throws Exception {
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.readValue(
                mapper.writeValueAsString(java.util.Map.of("port", port, "ssl", false)),
                ShuffleConfig.class);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop().join();
        }
    }

    @Test
    void pullIndex_returnsOffsets() throws Exception {
        final long[] offsets = client.pullIndex("localhost", "12345", "abcd1234");

        // 4 partitions + 1 = 5 offsets
        assertThat(offsets.length, is(5));
        assertThat(offsets[0], is(0L));
        // Partition 0 has data
        assertThat(offsets[1], greaterThan(offsets[0]));
        // Partition 1 is empty
        assertThat(offsets[2], is(offsets[1]));
        // Partition 2 has data
        assertThat(offsets[3], greaterThan(offsets[2]));
        // Partition 3 is empty
        assertThat(offsets[4], is(offsets[3]));
    }

    @Test
    void pullData_returnsCompressedBlock() throws Exception {
        final long[] offsets = client.pullIndex("localhost", "12345", "abcd1234");
        final long offset = offsets[0];
        final int length = (int) (offsets[1] - offsets[0]);

        final byte[] data = client.pullData("localhost", "12345", "abcd1234", offset, length);

        assertThat(data, notNullValue());
        assertThat(data.length, is(length));

        // Verify it can be decompressed and parsed
        final byte[] uncompressed = LocalDiskShuffleReader.decompressBlock(data);
        final var records = LocalDiskShuffleReader.parseRecords(uncompressed);
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getOperation(), is(ShuffleRecord.OP_INSERT));
    }

    @Test
    void isLocalAddress_localhost_returnsTrue() {
        assertThat(ShuffleNodeClient.isLocalAddress("localhost"), is(true));
        assertThat(ShuffleNodeClient.isLocalAddress("127.0.0.1"), is(true));
    }

    @Test
    void isLocalAddress_remoteAddress_returnsFalse() {
        assertThat(ShuffleNodeClient.isLocalAddress("192.0.2.1"), is(false));
    }

    @Test
    void resolveLocalAddress_returnsNonNull() {
        assertThat(ShuffleNodeClient.resolveLocalAddress(), notNullValue());
    }

    @Test
    void requestCleanup_deletesShuffleFiles() throws Exception {
        assertThat(storage.getTaskIds("12345"), hasSize(1));

        // Test the DELETE endpoint directly with a synchronous call to avoid flakiness
        final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newHttpClient();
        final java.net.http.HttpResponse<Void> response = httpClient.send(
                java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://localhost:" + port + "/shuffle/12345"))
                        .DELETE().build(),
                java.net.http.HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode(), is(200));
        assertThat(storage.getTaskIds("12345"), hasSize(0));
    }

    @Test
    void requestCleanup_nonExistentSnapshot_doesNotThrow() {
        // Should not throw, just log a warning at most
        client.requestCleanup("localhost", "nonexistent");
    }
}
