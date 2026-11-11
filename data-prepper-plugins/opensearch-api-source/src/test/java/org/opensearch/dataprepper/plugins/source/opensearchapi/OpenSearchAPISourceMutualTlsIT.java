/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.http.BaseHttpSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.MutualTlsArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.TestCertificateGenerator;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import javax.net.ssl.SSLException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end mutual TLS test for the real {@code opensearch_api} source.
 * <p>
 * The source configuration is bound from the same structure a pipeline YAML produces, bean validation runs on it,
 * and the real {@link OpenSearchAPISource} is started on a random port. Clients connect over real TLS.
 * <p>
 * The only stub is {@link PluginFactory}. The shared plugin test framework cannot load {@code @Experimental} plugins
 * from a source section, so the stub emulates the lookup: it checks the plugin name, binds and validates
 * {@link MutualTlsAuthenticationConfig}, and calls the real provider constructor.
 */
class OpenSearchAPISourceMutualTlsIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BULK_BODY =
            "{\"index\": {\"_index\": \"test-index\"}}\n{\"log\": \"mtls\"}\n";
    private static final Validator VALIDATOR = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory()
            .getValidator();

    @TempDir
    static Path tempDir;

    private static Path caCertFile;
    private static Path serverCertFile;
    private static Path serverKeyFile;
    private static Path trustedClientCertFile;
    private static Path trustedClientKeyFile;
    private static Path untrustedClientCertFile;
    private static Path untrustedClientKeyFile;

    private OpenSearchAPISource source;
    private BlockingBuffer<Record<Event>> buffer;

    @BeforeAll
    static void generateCertificates() throws Exception {
        final KeyPair caKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate caCert = TestCertificateGenerator.generateCaCertificate(caKeyPair);
        caCertFile = TestCertificateGenerator.writeCertificateToPem(caCert, tempDir.resolve("ca.crt"));

        final KeyPair serverKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate serverCert = TestCertificateGenerator.generateSignedCertificate(
                serverKeyPair, caKeyPair, caCert, "localhost");
        serverCertFile = TestCertificateGenerator.writeCertificateToPem(serverCert, tempDir.resolve("server.crt"));
        serverKeyFile = TestCertificateGenerator.writePrivateKeyToPem(serverKeyPair, tempDir.resolve("server.key"));

        final KeyPair clientKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate clientCert = TestCertificateGenerator.generateSignedCertificate(
                clientKeyPair, caKeyPair, caCert, "trusted-client");
        trustedClientCertFile = TestCertificateGenerator.writeCertificateToPem(clientCert, tempDir.resolve("client.crt"));
        trustedClientKeyFile = TestCertificateGenerator.writePrivateKeyToPem(clientKeyPair, tempDir.resolve("client.key"));

        // Signed by a different CA that the source does not trust.
        final KeyPair rogueCaKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate rogueCaCert = TestCertificateGenerator.generateCaCertificate(rogueCaKeyPair);
        final KeyPair rogueClientKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate rogueClientCert = TestCertificateGenerator.generateSignedCertificate(
                rogueClientKeyPair, rogueCaKeyPair, rogueCaCert, "untrusted-client");
        untrustedClientCertFile = TestCertificateGenerator.writeCertificateToPem(rogueClientCert, tempDir.resolve("rogue-client.crt"));
        untrustedClientKeyFile = TestCertificateGenerator.writePrivateKeyToPem(rogueClientKeyPair, tempDir.resolve("rogue-client.key"));
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.stop();
        }
    }

    @Test
    void trusted_client_certificate_is_accepted() throws Exception {
        final int port = startSource(false);

        try (ClientFactory factory = clientWithCertificate(trustedClientCertFile, trustedClientKeyFile)) {
            final AggregatedHttpResponse response = post(factory, port, "/_bulk", BULK_BODY);
            assertThat(response.status(), equalTo(HttpStatus.OK));

            final List<Record<Event>> records = new ArrayList<>(buffer.read(1000).getKey());
            assertThat(records, hasSize(1));
            assertThat(records.get(0).getData().get("log", String.class), equalTo("mtls"));
        }
    }

    @Test
    void client_without_certificate_receives_401() throws Exception {
        final int port = startSource(false);

        try (ClientFactory factory = clientWithoutCertificate()) {
            final AggregatedHttpResponse response = post(factory, port, "/_bulk", BULK_BODY);
            assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }
    }

    @Test
    void untrusted_client_certificate_fails_tls_handshake() throws Exception {
        final int port = startSource(false);

        // TLS 1.2: the server rejects the client certificate inside the handshake, so the client must
        // see an SSLException. Under TLS 1.3 the rejection can arrive after the client considers the
        // handshake done and surface only as a closed session, which this test must not accept.
        try (ClientFactory factory = tls12ClientWithCertificate(untrustedClientCertFile, untrustedClientKeyFile)) {
            final Exception exception = assertThrows(Exception.class,
                    () -> post(factory, port, "/_bulk", BULK_BODY));
            assertThat("Expected an SSLException in the cause chain", hasSslException(exception), is(true));
        }

        assertThat(buffer.read(100).getKey(), is(empty()));
    }

    @Test
    void untrusted_client_certificate_over_tls13_is_rejected_and_nothing_is_buffered() throws Exception {
        final int port = startSource(false);

        // Under TLS 1.3 the rejection may surface as an SSLException or as a closed session,
        // so only the outcome is asserted: the request fails and no record reaches the buffer.
        try (ClientFactory factory = tls13ClientWithCertificate(untrustedClientCertFile, untrustedClientKeyFile)) {
            assertThrows(Exception.class,
                    () -> post(factory, port, "/_bulk", BULK_BODY));
        }

        assertThat(buffer.read(100).getKey(), is(empty()));
    }

    // No health-check cases: OpenSearchAPISourceConfig.hasHealthCheckService() always returns false,
    // so this source never registers /health regardless of health_check_service.

    @Test
    void mutual_tls_without_ssl_fails_config_validation() {
        final Map<String, Object> settings = sourceSettings(false);
        settings.put("ssl", false);

        final OpenSearchAPISourceConfig config = OBJECT_MAPPER.convertValue(settings, OpenSearchAPISourceConfig.class);
        final Set<ConstraintViolation<OpenSearchAPISourceConfig>> violations = VALIDATOR.validate(config);

        assertThat(violations.stream().map(ConstraintViolation::getMessage).collect(Collectors.toList()),
                hasItem("ssl must be true when mutual_tls authentication is used"));
    }

    /**
     * Same keys as the pipeline YAML:
     * <pre>
     * opensearch_api:
     *   port: 0
     *   ssl: true
     *   ssl_certificate_file: ...
     *   ssl_key_file: ...
     *   health_check_service: true
     *   unauthenticated_health_check: ...
     *   authentication:
     *     mutual_tls:
     *       ssl_trust_certificate_file: ...
     * </pre>
     */
    private static Map<String, Object> sourceSettings(final boolean unauthenticatedHealthCheck) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("port", 0);
        settings.put("ssl", true);
        settings.put("ssl_certificate_file", serverCertFile.toString());
        settings.put("ssl_key_file", serverKeyFile.toString());
        settings.put("health_check_service", true);
        settings.put("unauthenticated_health_check", unauthenticatedHealthCheck);
        settings.put("authentication",
                Map.of("mutual_tls", Map.of("ssl_trust_certificate_file", caCertFile.toString())));
        return settings;
    }

    private int startSource(final boolean unauthenticatedHealthCheck) throws Exception {
        final OpenSearchAPISourceConfig config = OBJECT_MAPPER.convertValue(sourceSettings(unauthenticatedHealthCheck), OpenSearchAPISourceConfig.class);
        assertThat(VALIDATOR.validate(config), is(empty()));

        final String pipelineName = UUID.randomUUID().toString();
        final PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(pipelineName);

        final PluginFactory pluginFactory = mock(PluginFactory.class);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenAnswer(invocation -> loadAuthenticationPlugin(invocation.getArgument(1)));

        source = new OpenSearchAPISource(config, PluginMetrics.fromNames("opensearch_api", pipelineName), pluginFactory, pipelineDescription);
        buffer = new BlockingBuffer<>(10, 8, pipelineName);
        source.start(buffer);
        return activePort(source);
    }

    private static ArmeriaHttpAuthenticationProvider loadAuthenticationPlugin(final PluginSetting pluginSetting) {
        assertThat(pluginSetting.getName(), equalTo("mutual_tls"));
        final MutualTlsAuthenticationConfig pluginConfig =
                OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), MutualTlsAuthenticationConfig.class);
        assertThat(VALIDATOR.validate(pluginConfig), is(empty()));
        return new MutualTlsArmeriaHttpAuthenticationProvider(pluginConfig);
    }

    private static ClientFactory clientWithCertificate(final Path certFile, final Path keyFile) {
        return ClientFactory.builder()
                .tlsNoVerify()
                .tls(certFile.toFile(), keyFile.toFile())
                .build();
    }

    private static ClientFactory tls12ClientWithCertificate(final Path certFile, final Path keyFile) {
        return ClientFactory.builder()
                .tlsNoVerify()
                .tls(certFile.toFile(), keyFile.toFile())
                .tlsCustomizer(sslContextBuilder -> sslContextBuilder.protocols("TLSv1.2"))
                .build();
    }

    private static ClientFactory tls13ClientWithCertificate(final Path certFile, final Path keyFile) {
        return ClientFactory.builder()
                .tlsNoVerify()
                .tls(certFile.toFile(), keyFile.toFile())
                .tlsCustomizer(sslContextBuilder -> sslContextBuilder.protocols("TLSv1.3"))
                .build();
    }

    private static ClientFactory clientWithoutCertificate() {
        return ClientFactory.builder()
                .tlsNoVerify()
                .build();
    }

    private static AggregatedHttpResponse post(final ClientFactory factory, final int port, final String path, final String body) {
        return WebClient.builder().factory(factory).build()
                .execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTPS)
                                .authority("127.0.0.1:" + port)
                                .method(HttpMethod.POST)
                                .path(path)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(body))
                .aggregate()
                .join();
    }

    private static int activePort(final OpenSearchAPISource apiSource) throws Exception {
        final Field serverField = BaseHttpSource.class.getDeclaredField("server");
        serverField.setAccessible(true);
        return ((Server) serverField.get(apiSource)).activeLocalPort();
    }

    private static boolean hasSslException(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SSLException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
