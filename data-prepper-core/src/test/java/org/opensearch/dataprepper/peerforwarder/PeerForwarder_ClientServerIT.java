/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.CheckpointState;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests that verify that Peer Forwarder client-server communication
 * works.
 */
class PeerForwarder_ClientServerIT {

    public static final String LOCALHOST = "127.0.0.1";
    private ObjectMapper objectMapper;
    private String pipelineName;
    private String pluginId;
    private List<Record<Event>> outgoingRecords;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        outgoingRecords = IntStream.range(0, 5)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
    }

    private PeerForwarderServer createServer(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory,
            final PeerForwarderProvider peerForwarderProvider) {
        final PeerForwarderHttpService peerForwarderHttpService = new PeerForwarderHttpService(new ResponseHandler(), peerForwarderProvider, peerForwarderConfiguration, objectMapper);
        Objects.requireNonNull(peerForwarderConfiguration, "Nested classes must supply peerForwarderConfiguration");
        Objects.requireNonNull(certificateProviderFactory, "Nested classes must supply certificateProviderFactory");
        final PeerForwarderHttpServerProvider serverProvider = new PeerForwarderHttpServerProvider(peerForwarderConfiguration,
                certificateProviderFactory, peerForwarderHttpService);

        final Server server = serverProvider.get();

        return new RemotePeerForwarderServer(peerForwarderConfiguration, server);
    }

    private PeerForwarderProvider createPeerForwarderProvider(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory) {
        final PeerForwarderClient clientForProvider = createClient(peerForwarderConfiguration, certificateProviderFactory);
        final PeerClientPool peerClientPool = new PeerClientPool();
        final PeerForwarderClientFactory clientFactoryForProvider = new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory);
        return new PeerForwarderProvider(clientFactoryForProvider, clientForProvider, peerForwarderConfiguration);
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
        private PeerForwarderProvider peerForwarderProvider;

        @BeforeEach
        void setUp() {
            peerForwarderConfiguration = createConfiguration(true);

            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerForwarderProvider = createPeerForwarderProvider(peerForwarderConfiguration, certificateProviderFactory);
            peerForwarderProvider.register(pipelineName, pluginId, Collections.singleton(UUID.randomUUID().toString()));
            server = createServer(peerForwarderConfiguration, certificateProviderFactory, peerForwarderProvider);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @Test
        void send_Events_to_server() {
            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final AggregatedHttpResponse httpResponse = client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Map<String, PeerForwarderReceiveBuffer<Record<Event>>> pluginBufferMap = peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap().get(pipelineName);
            assertThat(pluginBufferMap, notNullValue());
            final PeerForwarderReceiveBuffer<Record<Event>> receiveBuffer = pluginBufferMap.get(pluginId);

            final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferEntry = receiveBuffer.read(1000);
            final Collection<Record<Event>> receivedRecords = bufferEntry.getKey();
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords.size(), equalTo(outgoingRecords.size()));

            final Set<String> receivedMessages = new HashSet<>();
            for (Record receivedRecord : receivedRecords) {
                assertThat(receivedRecord, notNullValue());
                assertThat(receivedRecord.getData(), instanceOf(Event.class));
                final Event event = (Event) receivedRecord.getData();
                final String message = event.get("message", String.class);
                assertThat(message, notNullValue());
                receivedMessages.add(message);
            }

            final Set<String> expectedMessages = outgoingRecords.stream()
                    .map(Record::getData)
                    .map(e -> e.get("message", String.class))
                    .collect(Collectors.toSet());

            assertThat(receivedMessages, equalTo(expectedMessages));
        }

        @Test
        void send_Events_to_server_when_client_does_not_expect_SSL_should_throw() {
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(false);
            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            assertThrows(ClosedSessionException.class, () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName));
        }
    }

    @Nested
    class WithoutSSL {

        private PeerForwarderConfiguration peerForwarderConfiguration;
        private CertificateProviderFactory certificateProviderFactory;
        private PeerForwarderServer server;
        private PeerForwarderProvider peerForwarderProvider;

        @BeforeEach
        void setUp() {
            peerForwarderConfiguration = createConfiguration(false);

            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerForwarderProvider = createPeerForwarderProvider(peerForwarderConfiguration, certificateProviderFactory);
            peerForwarderProvider.register(pipelineName, pluginId, Collections.singleton(UUID.randomUUID().toString()));
            server = createServer(peerForwarderConfiguration, certificateProviderFactory, peerForwarderProvider);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @Test
        void send_Events_to_server() {
            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final AggregatedHttpResponse httpResponse = client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Map<String, PeerForwarderReceiveBuffer<Record<Event>>> pluginBufferMap = peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap().get(pipelineName);
            assertThat(pluginBufferMap, notNullValue());
            final PeerForwarderReceiveBuffer<Record<Event>> receiveBuffer = pluginBufferMap.get(pluginId);

            final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferEntry = receiveBuffer.read(1000);
            final Collection<Record<Event>> receivedRecords = bufferEntry.getKey();
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords.size(), equalTo(outgoingRecords.size()));

            final Set<String> receivedMessages = new HashSet<>();
            for (Record receivedRecord : receivedRecords) {
                assertThat(receivedRecord, notNullValue());
                assertThat(receivedRecord.getData(), instanceOf(Event.class));
                final Event event = (Event) receivedRecord.getData();
                final String message = event.get("message", String.class);
                assertThat(message, notNullValue());
                receivedMessages.add(message);
            }

            final Set<String> expectedMessages = outgoingRecords.stream()
                    .map(Record::getData)
                    .map(e -> e.get("message", String.class))
                    .collect(Collectors.toSet());

            assertThat(receivedMessages, equalTo(expectedMessages));
        }

        @Test
        void send_Events_to_server_when_expecting_SSL_should_throw() {
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(true);
            certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration, certificateProviderFactory);

            final UnprocessedRequestException actualException = assertThrows(UnprocessedRequestException.class, () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName));

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
