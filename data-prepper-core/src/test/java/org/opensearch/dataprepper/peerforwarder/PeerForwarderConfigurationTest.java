/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_DRAIN_TIMEOUT;

import org.opensearch.dataprepper.TestDataProvider;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PRIVATE_KEY_FILE_PATH;

class PeerForwarderConfigurationTest {
    private static SimpleModule simpleModule = new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(simpleModule);

    private static PeerForwarderConfiguration makeConfig(final String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        return OBJECT_MAPPER.readValue(configurationFile, PeerForwarderConfiguration.class);
    }

    @Test
    void testPeerForwarderDefaultConfig() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_CONFIG_WITHOUT_SSL_FILE);

        assertThat(peerForwarderConfiguration.getServerPort(), equalTo(4994));
        assertThat(peerForwarderConfiguration.getRequestTimeout(), equalTo(10_000));
        assertThat(peerForwarderConfiguration.getServerThreadCount(), equalTo(200));
        assertThat(peerForwarderConfiguration.getMaxConnectionCount(), equalTo(500));
        assertThat(peerForwarderConfiguration.getMaxPendingRequests(), equalTo(1024));
        assertThat(peerForwarderConfiguration.isSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.isSslDisableVerification(), equalTo(false));
        assertThat(peerForwarderConfiguration.isSslFingerprintVerificationOnly(), equalTo(false));
        assertThat(peerForwarderConfiguration.getAcmPrivateKeyPassword(), equalTo(null));
        assertThat(peerForwarderConfiguration.isUseAcmCertificateForSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getDiscoveryMode(), equalTo(DiscoveryMode.LOCAL_NODE));
        assertThat(peerForwarderConfiguration.getClientThreadCount(), equalTo(200));
        assertThat(peerForwarderConfiguration.getBatchSize(), equalTo(48));
        assertThat(peerForwarderConfiguration.getBufferSize(), equalTo(512));
        assertThat(peerForwarderConfiguration.getAuthentication(), equalTo(ForwardingAuthentication.UNAUTHENTICATED));
        assertThat(peerForwarderConfiguration.getDrainTimeout(), equalTo(DEFAULT_DRAIN_TIMEOUT));
    }

    @Test
    void testValidPeerForwarderConfig() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_CONFIG_FILE);

        assertThat(peerForwarderConfiguration.getServerPort(), equalTo(21895));
        assertThat(peerForwarderConfiguration.getRequestTimeout(), equalTo(1000));
        assertThat(peerForwarderConfiguration.getServerThreadCount(), equalTo(100));
        assertThat(peerForwarderConfiguration.getMaxConnectionCount(), equalTo(100));
        assertThat(peerForwarderConfiguration.getMaxPendingRequests(), equalTo(512));
        assertThat(peerForwarderConfiguration.isSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.isSslDisableVerification(), equalTo(false));
        assertThat(peerForwarderConfiguration.isSslFingerprintVerificationOnly(), equalTo(false));
        assertThat(peerForwarderConfiguration.isUseAcmCertificateForSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getAcmCertificateArn(), equalTo(null));
        assertThat(peerForwarderConfiguration.getDiscoveryMode(), equalTo(DiscoveryMode.STATIC));
        assertThat(peerForwarderConfiguration.getStaticEndpoints(), equalTo(new ArrayList<>()));
        assertThat(peerForwarderConfiguration.getDomainName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getAwsCloudMapNamespaceName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getAwsCloudMapServiceName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getClientThreadCount(), equalTo(100));
        assertThat(peerForwarderConfiguration.getBatchSize(), equalTo(100));
        assertThat(peerForwarderConfiguration.getBufferSize(), equalTo(100));
        assertThat(peerForwarderConfiguration.getAuthentication(), equalTo(ForwardingAuthentication.UNAUTHENTICATED));
        assertThat(peerForwarderConfiguration.getDrainTimeout(), equalTo(DEFAULT_DRAIN_TIMEOUT));
    }

    @Test
    void testValidPeerForwarderConfig_with_Mutual_TLS() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig("src/test/resources/valid_peer_forwarder_config_with_mutual_tls.yml");

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
        assertThat(peerForwarderConfiguration.getAuthentication(), equalTo(ForwardingAuthentication.MUTUAL_TLS));
    }

    @Test
    void testValidPeerForwarderConfig_with_Unauthenticated() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig("src/test/resources/valid_peer_forwarder_config_with_unauthenticated.yml");

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getAuthentication(), equalTo(ForwardingAuthentication.UNAUTHENTICATED));
    }

    @Test
    void testValidPeerForwarderConfig_with_InsecureTls() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig("src/test/resources/valid_peer_forwarder_config_with_insecure.yml");

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
        assertThat(peerForwarderConfiguration.isSslDisableVerification(), equalTo(true));
    }

    @Test
    void testValidPeerForwarderConfig_with_FingerprintTls() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig("src/test/resources/valid_peer_forwarder_config_with_fingerprint.yml");

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
        assertThat(peerForwarderConfiguration.isSslFingerprintVerificationOnly(), equalTo(true));
    }

    @Test
    void testValidPeerForwarderConfig_with_DrainTimeout() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_CONFIG_WITH_DRAIN_TIMEOUT_FILE);

        assertThat(peerForwarderConfiguration.getDrainTimeout(), equalTo(Duration.ofSeconds(60)));
    }

    @Test
    void testValidPeerForwarderConfig_with_iso8601_DrainTimeout() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_CONFIG_WITH_ISO8601_DRAIN_TIMEOUT_FILE);

        assertThat(peerForwarderConfiguration.getDrainTimeout(), equalTo(Duration.ofSeconds(15)));
    }

    @Test
    void testInvalidPeerForwarderConfig_with_bad_DrainTimeout() {
        assertThrows(JsonMappingException.class, () -> makeConfig(TestDataProvider.INVALID_PEER_FORWARDER_WITH_BAD_DRAIN_TIMEOUT));
    }

    @Test
    void test_with_acm_should_create_PeerForwarderConfiguration_object_even_with_null_files() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_WITH_ACM_SSL_CONFIG_FILE);

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
        assertThat(peerForwarderConfiguration.isUseAcmCertificateForSsl(), equalTo(true));
    }

    @Test
    void test_cert_paths_with_ssl() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(TestDataProvider.INVALID_PEER_FORWARDER_WITH_SSL_CONFIG_FILE);

        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
        assertThat(peerForwarderConfiguration.getSslCertificateFile(), equalTo("invalid_path"));
        assertThat(peerForwarderConfiguration.getSslKeyFile(), equalTo(DEFAULT_PRIVATE_KEY_FILE_PATH));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_PORT_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_THREAD_COUNT_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_CONNECTION_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_DISCOVERY_MODE_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_BUFFER_SIZE_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_BATCH_SIZE_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_ARN_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_REGION_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_SERVICE_NAME_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_NAMESPACE_NAME_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_REGION_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_DNS_WITHOUT_DOMAIN_NAME_CONFIG_FILE,
            TestDataProvider.INVALID_PEER_FORWARDER_WITH_NEGATIVE_DRAIN_TIMEOUT,
            "src/test/resources/invalid_peer_forwarder_config_with_many_authentication.yml",
            "src/test/resources/invalid_peer_forwarder_config_with_mutual_tls_not_ssl.yml"
    })
    void invalid_InvalidPeerForwarderConfig_test(final String filePath) {
        assertThrows(ValueInstantiationException.class, () -> makeConfig(filePath));
    }
}