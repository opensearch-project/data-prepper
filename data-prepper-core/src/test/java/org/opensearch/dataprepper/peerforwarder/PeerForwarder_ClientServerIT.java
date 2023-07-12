/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.UnprocessedRequestException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ClosedSessionException;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.codec.PeerForwarderCodecAppConfig;
import org.opensearch.dataprepper.peerforwarder.codec.JacksonPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.JavaPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpServerProvider;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.RemotePeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.ResponseHandler;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.net.ssl.SSLHandshakeException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

/**
 * Integration tests that verify that Peer Forwarder client-server communication
 * works.
 */
class PeerForwarder_ClientServerIT {
    private static final int PIPELINE_WORKER_THREADS = new Random().nextInt(10) + 1;
    private static final String LOCALHOST = "127.0.0.1";
    private static final String SSL_CERTIFICATE_FILE = "src/test/resources/test-crt.crt";
    private static final String SSL_KEY_FILE = "src/test/resources/test-key.key";
    private static final String ALTERNATE_SSL_CERTIFICATE_FILE = "src/test/resources/test-alternate-crt.crt";
    private static final String ALTERNATE_SSL_KEY_FILE = "src/test/resources/test-alternate-key.key";
    private ObjectMapper objectMapper;
    private JavaPeerForwarderCodec javaPeerForwarderCodec;
    private JacksonPeerForwarderCodec jacksonPeerForwarderCodec;
    private PeerForwarderConfiguration peerForwarderConfiguration;
    private String pipelineName;
    private String pluginId;
    private List<Record<Event>> outgoingRecords;
    private Set<String> expectedMessages;
    private PluginMetrics pluginMetrics;
    private AcknowledgementSetManager acknowledgementSetManager; 
    private AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        outgoingRecords = IntStream.range(0, 5)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();

        pluginMetrics = PluginMetrics.fromNames(pluginId, pipelineName);

        expectedMessages = outgoingRecords.stream()
                .map(Record::getData)
                .map(e -> e.get("message", String.class))
                .collect(Collectors.toSet());
    }

    private void setupApplicationContext() {
        applicationContext = new AnnotationConfigApplicationContext();

        applicationContext.scan(PeerForwarderCodecAppConfig.class.getPackage().getName());
        applicationContext.registerBean("peerForwarderConfiguration", PeerForwarderConfiguration.class, () -> peerForwarderConfiguration);
        applicationContext.refresh();
    }


    private PeerForwarderServer createServer(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory,
            final PeerForwarderProvider peerForwarderProvider) {
        final PeerForwarderCodec peerForwarderCodec = applicationContext.getBean(PeerForwarderCodec.class);
        final PeerForwarderHttpService peerForwarderHttpService = new PeerForwarderHttpService(new ResponseHandler(pluginMetrics), peerForwarderProvider, peerForwarderConfiguration,
                peerForwarderCodec, acknowledgementSetManager, pluginMetrics);
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
        final PeerForwarderClient clientForProvider = createClient(peerForwarderConfiguration);
        final PeerClientPool peerClientPool = new PeerClientPool();
        final PeerForwarderClientFactory clientFactoryForProvider = new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory, pluginMetrics);
        return new PeerForwarderProvider(clientFactoryForProvider, clientForProvider, peerForwarderConfiguration, pluginMetrics);
    }

    private PeerForwarderClient createClient(
            final PeerForwarderConfiguration peerForwarderConfiguration) {
        Objects.requireNonNull(peerForwarderConfiguration, "Nested classes must supply peerForwarderConfiguration");
        final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
        final PeerClientPool peerClientPool = new PeerClientPool();
        final PeerForwarderClientFactory peerForwarderClientFactory = new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory, pluginMetrics);
        peerForwarderClientFactory.setPeerClientPool();
        final PeerForwarderCodec peerForwarderCodec = applicationContext.getBean(PeerForwarderCodec.class);
        return new PeerForwarderClient(peerForwarderConfiguration, peerForwarderClientFactory, peerForwarderCodec, pluginMetrics);
    }

    private Collection<Record<Event>> getServerSideRecords(final PeerForwarderProvider peerForwarderProvider) {
        final Map<String, PeerForwarderReceiveBuffer<Record<Event>>> pluginBufferMap = peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap().get(pipelineName);
        assertThat(pluginBufferMap, notNullValue());
        final PeerForwarderReceiveBuffer<Record<Event>> receiveBuffer = pluginBufferMap.get(pluginId);

        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferEntry = receiveBuffer.read(400);
        return bufferEntry.getKey();
    }

    @Nested
    class WithSSL {
        private PeerForwarderServer server;
        private PeerForwarderProvider peerForwarderProvider;

        void setUpServer(final boolean binaryCodec) {
            peerForwarderConfiguration = createConfiguration(true, ForwardingAuthentication.UNAUTHENTICATED, binaryCodec);

            setupApplicationContext();

            final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerForwarderProvider = createPeerForwarderProvider(peerForwarderConfiguration, certificateProviderFactory);
            peerForwarderProvider.register(pipelineName, pluginId, Collections.singleton(UUID.randomUUID().toString()), PIPELINE_WORKER_THREADS);
            server = createServer(peerForwarderConfiguration, certificateProviderFactory, peerForwarderProvider);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server(final boolean binaryCodec) throws ExecutionException, InterruptedException {
            setUpServer(binaryCodec);
            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final CompletableFuture<AggregatedHttpResponse> httpResponseFuture =
                    client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);
            final AggregatedHttpResponse httpResponse = httpResponseFuture.get();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            validatePeerForwarderBufferRecords(receivedRecords);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_when_client_does_not_expect_SSL_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    false, ForwardingAuthentication.UNAUTHENTICATED, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(ClosedSessionException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_an_unknown_server_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.UNAUTHENTICATED,
                    ALTERNATE_SSL_CERTIFICATE_FILE, ALTERNATE_SSL_KEY_FILE, false, false, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_with_fingerprint_verification(final boolean binaryCodec) throws ExecutionException, InterruptedException {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                        true, ForwardingAuthentication.UNAUTHENTICATED, SSL_CERTIFICATE_FILE, SSL_KEY_FILE,
                    false, true, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final CompletableFuture<AggregatedHttpResponse> httpResponseFuture =
                    client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);
            final AggregatedHttpResponse httpResponse = httpResponseFuture.get();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            validatePeerForwarderBufferRecords(receivedRecords);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_with_fingerprint_verification_to_unknown_server_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.UNAUTHENTICATED, ALTERNATE_SSL_CERTIFICATE_FILE,
                    ALTERNATE_SSL_KEY_FILE, false, true, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }
    }

    @Nested
    class WithoutSSL {
        private PeerForwarderServer server;
        private PeerForwarderProvider peerForwarderProvider;

        void setUpServer(final boolean binaryCodec) {
            peerForwarderConfiguration = createConfiguration(false, ForwardingAuthentication.UNAUTHENTICATED, binaryCodec);

            setupApplicationContext();

            final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerForwarderProvider = createPeerForwarderProvider(peerForwarderConfiguration, certificateProviderFactory);
            peerForwarderProvider.register(pipelineName, pluginId, Collections.singleton(UUID.randomUUID().toString()), PIPELINE_WORKER_THREADS);
            server = createServer(peerForwarderConfiguration, certificateProviderFactory, peerForwarderProvider);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server(final boolean binaryCodec) throws ExecutionException, InterruptedException {
            setUpServer(binaryCodec);
            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final CompletableFuture<AggregatedHttpResponse> httpResponseFuture =
                    client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);
            final AggregatedHttpResponse httpResponse = httpResponseFuture.get();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            validatePeerForwarderBufferRecords(receivedRecords);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_when_expecting_SSL_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.UNAUTHENTICATED, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }
    }

    @Nested
    class WithMutualTls {
        private PeerForwarderServer server;
        private PeerForwarderProvider peerForwarderProvider;

        void setUpServer(final boolean binaryCodec) {
            peerForwarderConfiguration = createConfiguration(true, ForwardingAuthentication.MUTUAL_TLS, binaryCodec);

            setupApplicationContext();

            final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerForwarderProvider = createPeerForwarderProvider(peerForwarderConfiguration, certificateProviderFactory);
            peerForwarderProvider.register(pipelineName, pluginId, Collections.singleton(UUID.randomUUID().toString()), PIPELINE_WORKER_THREADS);
            server = createServer(peerForwarderConfiguration, certificateProviderFactory, peerForwarderProvider);
            server.start();
        }

        @AfterEach
        void tearDown() {
            server.stop();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server(final boolean binaryCodec) throws ExecutionException, InterruptedException {
            setUpServer(binaryCodec);
            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final CompletableFuture<AggregatedHttpResponse> httpResponseFuture =
                    client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);
            final AggregatedHttpResponse httpResponse = httpResponseFuture.get();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            validatePeerForwarderBufferRecords(receivedRecords);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_when_client_has_no_certificate_closes(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    false, ForwardingAuthentication.UNAUTHENTICATED, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(ClosedSessionException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_when_client_has_unknown_certificate_key_closes(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.MUTUAL_TLS, SSL_CERTIFICATE_FILE, ALTERNATE_SSL_KEY_FILE,
                    true, false, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);
            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_an_unknown_server_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.MUTUAL_TLS, ALTERNATE_SSL_CERTIFICATE_FILE, SSL_KEY_FILE,
                    true, false, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_to_server_with_fingerprint_verification(final boolean binaryCodec)
                throws ExecutionException, InterruptedException {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.MUTUAL_TLS, SSL_CERTIFICATE_FILE, SSL_KEY_FILE, false, true,
                    binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final CompletableFuture<AggregatedHttpResponse> httpResponseFuture =
                    client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName);
            final AggregatedHttpResponse httpResponse = httpResponseFuture.get();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            validatePeerForwarderBufferRecords(receivedRecords);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void send_Events_with_fingerprint_verification_to_unknown_server_should_throw(final boolean binaryCodec) {
            setUpServer(binaryCodec);
            final PeerForwarderConfiguration peerForwarderConfiguration = createConfiguration(
                    true, ForwardingAuthentication.MUTUAL_TLS, ALTERNATE_SSL_CERTIFICATE_FILE, SSL_KEY_FILE,
                    false, true, binaryCodec);

            final PeerForwarderClient client = createClient(peerForwarderConfiguration);

            final ExecutionException actualException = assertThrows(ExecutionException.class,
                    () -> client.serializeRecordsAndSendHttpRequest(outgoingRecords, LOCALHOST, pluginId, pipelineName).get());

            assertThat(actualException.getCause(), instanceOf(UnprocessedRequestException.class));
            assertThat(actualException.getCause().getCause(), instanceOf(SSLHandshakeException.class));

            final Collection<Record<Event>> receivedRecords = getServerSideRecords(peerForwarderProvider);
            assertThat(receivedRecords, notNullValue());
            assertThat(receivedRecords, is(empty()));
        }
    }

    private void validatePeerForwarderBufferRecords(final Collection<Record<Event>> receivedRecords) {
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

        assertThat(receivedMessages, equalTo(expectedMessages));
    }

    private PeerForwarderConfiguration createConfiguration(final boolean ssl,
                                                           final ForwardingAuthentication authentication,
                                                           final boolean binaryCodec) {
        return createConfiguration(ssl, authentication, SSL_CERTIFICATE_FILE, SSL_KEY_FILE, true, false, binaryCodec);
    }

    private PeerForwarderConfiguration createConfiguration(
            final boolean ssl,
            final ForwardingAuthentication authentication,
            final String sslCertificateFile,
            final String sslKeyFile,
            final boolean sslDisableVerification,
            final boolean sslFingerprintVerificationOnly,
            final boolean binaryCodec) {
        final Map<String, Object> authenticationMap = Collections.singletonMap(authentication.getName(), null);
        return new PeerForwarderConfiguration(
                4994,
                10_000,
                60_000,
                200,
                500,
                1024,
                ssl,
                sslCertificateFile,
                sslKeyFile,
                sslDisableVerification,
                sslFingerprintVerificationOnly,
                authenticationMap,
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
                3000,
                512,
                null,
                null,
                null,
                null,
                null,
                binaryCodec
        );
    }
}
