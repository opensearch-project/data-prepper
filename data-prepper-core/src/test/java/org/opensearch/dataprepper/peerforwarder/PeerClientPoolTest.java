/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import com.linecorp.armeria.client.WebClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

@ExtendWith(MockitoExtension.class)
class PeerClientPoolTest {
    private static final String VALID_ADDRESS = "10.10.10.5";
    private static final String LOCALHOST = "localhost";
    private static final int PORT = 21890;

    @Test
    void testGetClientValidAddress() {
        PeerClientPool pool = new PeerClientPool();
        pool.setPort(PORT);

        WebClient client = pool.getClient(VALID_ADDRESS);

        Assertions.assertNotNull(client);
    }

    @Test
    void testGetClientWithSSL() throws IOException {
        PeerClientPool pool = new PeerClientPool();
        pool.setSsl(true);
        pool.setPort(PORT);

        final Path certFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-crt.crt")).getFile()).toPath();
        final Path keyFilePath = new File(Objects.requireNonNull(PeerClientPoolTest.class.getClassLoader().getResource("test-key.key")).getFile()).toPath();
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);
        final Certificate certificate = new Certificate(certAsString, keyAsString);

        pool.setCertificate(certificate);

        WebClient client = pool.getClient(LOCALHOST);

        Assertions.assertNotNull(client);
    }

}
