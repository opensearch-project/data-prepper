/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.client.WebClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class PeerClientPoolTest {
    private static final String VALID_ADDRESS = "10.10.10.5";
    private static final String LOCALHOST = "localhost";
    private static final int PORT = 4994;

    @ParameterizedTest
    @ValueSource(strings = {VALID_ADDRESS, LOCALHOST})
    void testGetClientValidAddress(final String address) {
        PeerClientPool pool = new PeerClientPool();
        pool.setPort(PORT);

        WebClient client = pool.getClient(address);

        Assertions.assertNotNull(client);
        assertThat(client.uri(), equalTo(URI.create("http://" + address + ":" + PORT + "/")));
    }

    @ParameterizedTest
    @ValueSource(strings = {VALID_ADDRESS, LOCALHOST})
    void testGetClientWithSSL(final String address) throws IOException {
        PeerClientPool pool = new PeerClientPool();
        pool.setSsl(true);
        pool.setPort(PORT);

        final Path certFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-crt.crt")).getFile()).toPath();
        final Path keyFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-key.key")).getFile()).toPath();
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);
        final Certificate certificate = new Certificate(certAsString, keyAsString);

        pool.setCertificate(certificate);

        WebClient client = pool.getClient(address);

        Assertions.assertNotNull(client);
        assertThat(client.uri(), equalTo(URI.create("https://" + address + ":" + PORT + "/")));
    }

    @ParameterizedTest
    @ValueSource(strings = {VALID_ADDRESS, LOCALHOST})
    void testGetClientWithMutualTls(final String address) throws IOException {
        final PeerClientPool objectUnderTest = new PeerClientPool();
        objectUnderTest.setSsl(true);
        objectUnderTest.setPort(PORT);
        objectUnderTest.setAuthentication(ForwardingAuthentication.MUTUAL_TLS);

        final Path certFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-crt.crt")).getFile()).toPath();
        final Path keyFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-key.key")).getFile()).toPath();
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);
        final Certificate certificate = new Certificate(certAsString, keyAsString);

        objectUnderTest.setCertificate(certificate);

        final WebClient client = objectUnderTest.getClient(address);

        assertThat(client, notNullValue());
        assertThat(client.uri(), equalTo(URI.create("https://" + address + ":" + PORT + "/")));
    }

}
