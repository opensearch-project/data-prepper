/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.client.UnprocessedRequestException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ClosedSessionException;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpServerProvider;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.RemotePeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.ResponseHandler;

import javax.net.ssl.SSLHandshakeException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests that verify that Peer Forwarder client-server communication
 * works.
 */
class PeerForwarder_ClientServerIT {

    public static final String LOCALHOST = "127.0.0.1";
    private PeerForwarderHttpService peerForwarderHttpService;
    private ObjectMapper objectMapper;
    private String pluginId;
    private List<Record<Event>> records;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        peerForwarderHttpService = new PeerForwarderHttpService(new ResponseHandler(), objectMapper);

        records = IntStream.range(0, 5)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());
        pluginId = UUID.randomUUID().toString();
    }

    private PeerForwarderServer createServer(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory) {
        Objects.requireNonNull(peerForwarderConfiguration, "Nested classes must supply peerForwarderConfiguration");
        Objects.requireNonNull(certificateProviderFactory, "Nested classes must supply certificateProviderFactory");
        final PeerForwarderHttpServerProvider serverProvider = new PeerForwarderHttpServerProvider(peerForwarderConfiguration,
                certificateProviderFactory, peerForwarderHttpService);

        final Server server = serverProvider.get();

        return new RemotePeerForwarderServer(peerForwarderConfiguration, server);
    }

    private PeerForwarderClient createClient(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory) {
        Objects.requireNonNull(peerForwarderConfiguration, "Nested classes must supply peerForwarderConfiguration");
        Objects.requireNonNull(certificateProviderFactory, "Nested classes must supply certificateProviderFactory");
        final PeerClientPool peerClientPool = new PeerClientPool();
        final PeerForwarderClientFactory peerForwarderClientFactory = new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory);
        peerForwarderClientFactory.setPeerClientPool();
        return new PeerForwarderClient(peerForwarderConfiguration, peerForwarderClientFactory, objectMapper);
    }

    @Nested
    class WithSSL {

        private PeerForwarderConfiguration peerForwarderConfiguration;
        private CertificateProviderFactory certificateProviderFactory;
        private PeerForwarderServer server;

        @BeforeEach
        void setUp() {
            peerForwarderConfiguration = createConfiguration(true);

            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            server = createServer(peerForwarderConfiguration, certificateProviderFactory);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @Test
        void send_Events_to_server() {
            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final AggregatedHttpResponse httpResponse = client.serializeRecordsAndSendHttpRequest(records, LOCALHOST, pluginId);

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            // TODO: Validate that Events are received on the Server
        }

        @Test
        void send_Events_to_server_when_client_does_not_expect_SSL_should_throw() {
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(false);
            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            assertThrows(ClosedSessionException.class, () -> client.serializeRecordsAndSendHttpRequest(records, LOCALHOST, pluginId));
        }
    }

    @Nested
    class WithoutSSL {

        private PeerForwarderConfiguration peerForwarderConfiguration;
        private CertificateProviderFactory certificateProviderFactory;
        private PeerForwarderServer server;

        @BeforeEach
        void setUp() {
            peerForwarderConfiguration = createConfiguration(false);

            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            server = createServer(peerForwarderConfiguration, certificateProviderFactory);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @Test
        void send_Events_to_server() {
            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final AggregatedHttpResponse httpResponse = client.serializeRecordsAndSendHttpRequest(records, LOCALHOST, pluginId);

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            // TODO: Validate that Events are received on the Server
        }

        @Test
        void send_Events_to_server_when_expecting_SSL_should_throw() {
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(true);
            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final UnprocessedRequestException actualException = assertThrows(UnprocessedRequestException.class, () -> client.serializeRecordsAndSendHttpRequest(records, LOCALHOST, pluginId));

            assertThat(actualException.getCause(), instanceOf(SSLHandshakeException.class));
        }
    }

    private PeerForwarderConfiguration createConfiguration(final boolean ssl) {
        final String sslCertificateFile = "src/test/resources/test-crt.crt";
        final String sslKeyFile = "src/test/resources/test-key.key";
        return new PeerForwarderConfiguration(
                21890,
                10_000,
                200,
                500,
                1024,
                ssl,
                sslCertificateFile,
                sslKeyFile,
                false,
                null,
                null,
                120000,
                DiscoveryMode.STATIC.toString(),
                null,
                null,
                null,
                null,
                null,
                List.of("127.0.0.1"),
                200,
                48,
                512
        );
    }
}
