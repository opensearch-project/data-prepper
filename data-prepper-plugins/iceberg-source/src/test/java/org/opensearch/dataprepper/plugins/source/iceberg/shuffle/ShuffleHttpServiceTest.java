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

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ShuffleHttpServiceTest {

    private Path tempDir;
    private Server server;
    private ClientFactory clientFactory;
    private WebClient client;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("shuffle-http-test");
        final LocalDiskShuffleStorage storage = new LocalDiskShuffleStorage(tempDir);
        final ShuffleHttpService service = new ShuffleHttpService(storage);

        final ServerBuilder sb = Server.builder();
        sb.http(0);
        sb.annotatedService("/shuffle", service);
        server = sb.build();
        server.start().join();

        clientFactory = ClientFactory.builder().build();
        client = WebClient.builder("http://127.0.0.1:" + server.activeLocalPort())
                .factory(clientFactory)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (clientFactory != null) {
            clientFactory.close();
        }
        if (server != null) {
            server.stop().join();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"../etc/passwd", "abc", "12.34", "-1"})
    void getIndex_returns_bad_request_for_invalid_snapshotId(final String snapshotId) {
        final AggregatedHttpResponse response = client.get("/shuffle/" + snapshotId + "/abcd1234/index").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @ParameterizedTest
    @ValueSource(strings = {"../etc/passwd", "ABCD", "task-id"})
    void getIndex_returns_bad_request_for_invalid_taskId(final String taskId) {
        final AggregatedHttpResponse response = client.get("/shuffle/12345/" + taskId + "/index").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void getData_returns_bad_request_for_negative_offset() {
        final AggregatedHttpResponse response = client.get("/shuffle/12345/abcd1234/data?offset=-1&length=10").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void getData_returns_bad_request_for_negative_length() {
        final AggregatedHttpResponse response = client.get("/shuffle/12345/abcd1234/data?offset=0&length=-1").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @ParameterizedTest
    @ValueSource(strings = {"../etc", "abc", "12.34"})
    void cleanup_returns_bad_request_for_invalid_snapshotId(final String snapshotId) {
        final AggregatedHttpResponse response = client.delete("/shuffle/" + snapshotId).aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void getIndex_returns_not_found_for_nonexistent_file() {
        final AggregatedHttpResponse response = client.get("/shuffle/99999/abcd1234/index").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.NOT_FOUND));
    }

    @Test
    void getData_returns_ok_for_zero_length() {
        final AggregatedHttpResponse response = client.get("/shuffle/12345/abcd1234/data?offset=0&length=0").aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.OK));
    }
}
