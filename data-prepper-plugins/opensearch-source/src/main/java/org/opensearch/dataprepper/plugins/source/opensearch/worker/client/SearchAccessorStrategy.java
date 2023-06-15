/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.client.util.MissingRequiredPropertyException;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Objects;

/**
 * SearchAccessorStrategy determines which {@link SearchAccessor} (Elasticsearch or OpenSearch) should be used based on
 * the {@link OpenSearchSourceConfiguration}.
 * @since 2.4
 */
public class SearchAccessorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SearchAccessorStrategy.class);

    private static final String AOS_SERVICE_NAME = "es";

    static final String OPENSEARCH_DISTRIBUTION = "opensearch";
    static final String ELASTICSEARCH_DISTRIBUTION = "elasticsearch";
    static final String ELASTICSEARCH_OSS_BUILD_FLAVOR = "oss";
    static final String OPENDISTRO_DISTRIUBTION = "opendistro";

    private static final String OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF = "2.5.0";
    private static final String ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF = "7.10.0";


    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    public static SearchAccessorStrategy create(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                final AwsCredentialsSupplier awsCredentialsSupplier) {
        return new SearchAccessorStrategy(openSearchSourceConfiguration, awsCredentialsSupplier);
    }

    private SearchAccessorStrategy(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                  final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    /**
     * Provides a {@link SearchAccessor} that is based on the {@link OpenSearchSourceConfiguration}
     * @return a {@link SearchAccessor}
     * @since 2.4
     */
    public SearchAccessor getSearchAccessor() {

        OpenSearchTransport transport;
        if (Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions())) {
            transport = createOpenSearchTransportForAws();
        } else {
            final RestClient restClient = createOpenSearchRestClient();
            transport = createOpenSearchTransport(restClient);
        }
        final OpenSearchClient openSearchClient = new OpenSearchClient(transport);

        InfoResponse infoResponse = null;

        ElasticsearchClient elasticsearchClient = null;
        try {
            infoResponse = openSearchClient.info();
        } catch (final MissingRequiredPropertyException e) {
            LOG.info("Detected Elasticsearch cluster. Constructing Elasticsearch client");
            final org.elasticsearch.client.RestClient restClientElasticsearch = createElasticSearchRestClient();
            final ElasticsearchTransport elasticsearchTransport = createElasticSearchTransport(restClientElasticsearch);
            elasticsearchClient = new ElasticsearchClient(elasticsearchTransport);
        } catch (final IOException | OpenSearchException e) {
            throw new RuntimeException("There was an error looking up the OpenSearch cluster info: ", e);
        }

        final Pair<String, String> distributionAndVersion = getDistributionAndVersionNumber(infoResponse, elasticsearchClient);

        final String distribution = distributionAndVersion.getLeft();
        final String versionNumber = distributionAndVersion.getRight();

        validateDistribution(distribution);

        SearchContextType searchContextType;

        if (Objects.nonNull(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
            LOG.info("Using search_context_type set in the config: '{}'", openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType().toString().toLowerCase());
            validateSearchContextTypeOverride(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType(), distribution, versionNumber);
            searchContextType = openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType();
        } else if (versionSupportsPointInTime(distribution, versionNumber)) {
            LOG.info("{} distribution and version {} detected. Point in time APIs will be used to search documents", distribution, versionNumber);
            searchContextType = SearchContextType.POINT_IN_TIME;
        } else {
            LOG.info("{} distribution, version {} detected. Scroll contexts will be used to search documents. " +
                    "Upgrade your cluster to at least version {} to use Point in Time APIs instead of scroll.", distribution, versionNumber,
                    distribution.equals(ELASTICSEARCH_DISTRIBUTION) ? ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF : OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
            searchContextType = SearchContextType.SCROLL;
        }

        if (Objects.isNull(elasticsearchClient)) {
            return new OpenSearchAccessor(openSearchClient, searchContextType);
        }

        return new ElasticsearchAccessor(elasticsearchClient, searchContextType);
    }

    private RestClient createOpenSearchRestClient() {
        final List<String> hosts = openSearchSourceConfiguration.getHosts();
        final HttpHost[] httpHosts = new HttpHost[hosts.size()];

        int i = 0;
        for (final String host : hosts) {
            httpHosts[i] = HttpHost.create(host);
            i++;
        }

        final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);

        LOG.info("Using username and password for auth for the OpenSearch source");
        attachUsernamePassword(restClientBuilder);

        setConnectAndSocketTimeout(restClientBuilder);

        return restClientBuilder.build();
    }

    private void attachSSLContext(final ApacheHttpClient.Builder apacheHttpClientBuilder) {
        TrustManager[] trustManagers = createTrustManagers(openSearchSourceConfiguration.getConnectionConfiguration().getCertPath());
        apacheHttpClientBuilder.tlsTrustManagersProvider(() -> trustManagers);
    }

    private void attachSSLContext(final HttpAsyncClientBuilder httpClientBuilder) {

        final ConnectionConfiguration connectionConfiguration = openSearchSourceConfiguration.getConnectionConfiguration();
        final SSLContext sslContext = Objects.nonNull(connectionConfiguration.getCertPath()) ? getCAStrategy(connectionConfiguration.getCertPath()) : getTrustAllStrategy();
        httpClientBuilder.setSSLContext(sslContext);

        if (connectionConfiguration.isInsecure()) {
            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
    }

    private static TrustManager[] createTrustManagers(final Path certPath) {
        if (certPath != null) {
            LOG.info("Using the cert provided in the config.");
            try (InputStream certificateInputStream = Files.newInputStream(certPath)) {
                final CertificateFactory factory = CertificateFactory.getInstance("X.509");
                final Certificate trustedCa = factory.generateCertificate(certificateInputStream);
                final KeyStore trustStore = KeyStore.getInstance("pkcs12");
                trustStore.load(null, null);
                trustStore.setCertificateEntry("ca", trustedCa);

                final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
                trustManagerFactory.init(trustStore);
                return trustManagerFactory.getTrustManagers();
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        } else {
            return new TrustManager[] { new X509TrustAllManager() };
        }
    }

    private void attachUsernamePassword(final RestClientBuilder restClientBuilder) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(openSearchSourceConfiguration.getUsername(), openSearchSourceConfiguration.getPassword()));

        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            attachSSLContext(httpClientBuilder);
            return httpClientBuilder;
        });
    }

    private void setConnectAndSocketTimeout(final RestClientBuilder restClientBuilder) {
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
            if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout())) {
                requestConfigBuilder.setConnectTimeout((int) openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout().toMillis());
            }

            if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout())) {
                requestConfigBuilder.setSocketTimeout((int) openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout().toMillis());
            }

            return requestConfigBuilder;
        });
    }

    private OpenSearchTransport createOpenSearchTransport(final RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    private OpenSearchTransport createOpenSearchTransportForAws() {
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                .withStsRoleArn(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .withStsExternalId(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId())
                .withStsHeaderOverrides(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build());

        return new AwsSdk2Transport(createSdkHttpClient(),
                HttpHost.create(openSearchSourceConfiguration.getHosts().get(0)).getHostName(),
                AOS_SERVICE_NAME, openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion(),
                AwsSdk2TransportOptions.builder()
                        .setCredentials(awsCredentialsProvider)
                        .setMapper(new JacksonJsonpMapper())
                        .build());
    }

    private SdkHttpClient createSdkHttpClient() {
        final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();

        if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout())) {
            apacheHttpClientBuilder.connectionTimeout(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout());
        }

        if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout())) {
            apacheHttpClientBuilder.socketTimeout(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout());
        }

        attachSSLContext(apacheHttpClientBuilder);

        return apacheHttpClientBuilder.build();
    }

    private SSLContext getCAStrategy(final Path certPath) {
        LOG.info("Using the cert provided in the config.");
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = Files.newInputStream(certPath)) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private SSLContext getTrustAllStrategy() {
        LOG.info("Using the trust all strategy");
        final TrustStrategy trustStrategy = new TrustAllStrategy();
        try {
            return SSLContexts.custom().loadTrustMaterial(null, trustStrategy).build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private void validateSearchContextTypeOverride(final SearchContextType searchContextType, final String distribution, final String version) {

        if (searchContextType.equals(SearchContextType.POINT_IN_TIME) && !versionSupportsPointInTime(distribution, version)) {
            throw new IllegalArgumentException(
                    String.format("A search_context_type of point_in_time is only supported on OpenSearch versions %s and above. " +
                    "The version of the OpenSearch cluster passed is %s. Elasticsearch clusters with build-flavor %s do not support point in time",
                            distribution.startsWith(ELASTICSEARCH_DISTRIBUTION) ? ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF : OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF,
                            version, ELASTICSEARCH_OSS_BUILD_FLAVOR));
        }
    }

    private boolean versionSupportsPointInTime(final String distribution, final String version) {
        final DefaultArtifactVersion actualVersion = new DefaultArtifactVersion(version);

        DefaultArtifactVersion cutoffVersion;
        if (distribution.startsWith(ELASTICSEARCH_DISTRIBUTION)) {
            if (distribution.endsWith(ELASTICSEARCH_OSS_BUILD_FLAVOR)) {
                return false;
            }
            cutoffVersion = new DefaultArtifactVersion(ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
        } else {
            cutoffVersion = new DefaultArtifactVersion(OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
        }
        return actualVersion.compareTo(cutoffVersion) >= 0;
    }

    private Pair<String, String> getDistributionAndVersionNumber(final InfoResponse infoResponseOpenSearch, final ElasticsearchClient elasticsearchClient) {
        if (Objects.nonNull(infoResponseOpenSearch)) {
            return Pair.of(infoResponseOpenSearch.version().distribution(), infoResponseOpenSearch.version().number());
        }

        try {
            final co.elastic.clients.elasticsearch.core.InfoResponse infoResponseElasticsearch = elasticsearchClient.info();
            return Pair.of(ELASTICSEARCH_DISTRIBUTION + "-" + infoResponseElasticsearch.version().buildFlavor(), infoResponseElasticsearch.version().number());
        } catch (final Exception e) {
            throw new RuntimeException("Unable to call info API using the elasticsearch client", e);
        }
    }

    private void validateDistribution(final String distribution) {
        if (!distribution.equals(OPENSEARCH_DISTRIBUTION) && !distribution.startsWith(ELASTICSEARCH_DISTRIBUTION) && !distribution.equals(OPENDISTRO_DISTRIUBTION)) {
            throw new IllegalArgumentException(String.format("Only %s, %s, or %s distributions are supported at this time. The cluster distribution being used is '%s'",
                    OPENSEARCH_DISTRIBUTION, OPENDISTRO_DISTRIUBTION, ELASTICSEARCH_DISTRIBUTION, distribution));
        }
    }

    private ElasticsearchTransport createElasticSearchTransport(final org.elasticsearch.client.RestClient restClient) {
        return new co.elastic.clients.transport.rest_client.RestClientTransport(restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper());
    }

    private org.elasticsearch.client.RestClient createElasticSearchRestClient() {
        final List<String> hosts = openSearchSourceConfiguration.getHosts();
        final HttpHost[] httpHosts = new HttpHost[hosts.size()];

        int i = 0;
        for (final String host : hosts) {
            httpHosts[i] = HttpHost.create(host);
            i++;
        }

        final org.elasticsearch.client.RestClientBuilder restClientBuilder = org.elasticsearch.client.RestClient.builder(httpHosts);

        restClientBuilder.setDefaultHeaders(new Header[] {
                new BasicHeader("Content-type", "application/json")
        });

        LOG.info("Using username and password for auth for the OpenSearch source");
        attachUsernamePassword(restClientBuilder);

        setConnectAndSocketTimeout(restClientBuilder);

        return restClientBuilder.build();
    }

    private void attachUsernamePassword(final org.elasticsearch.client.RestClientBuilder restClientBuilder) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(openSearchSourceConfiguration.getUsername(), openSearchSourceConfiguration.getPassword()));

        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            attachSSLContext(httpClientBuilder);
            httpClientBuilder.addInterceptorLast(
                    (HttpResponseInterceptor)
                            (response, context) ->
                                    response.addHeader("X-Elastic-Product", "Elasticsearch"));
            return httpClientBuilder;
        });
    }

    private void setConnectAndSocketTimeout(final org.elasticsearch.client.RestClientBuilder restClientBuilder) {
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
            if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout())) {
                requestConfigBuilder.setConnectTimeout((int) openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout().toMillis());
            }

            if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout())) {
                requestConfigBuilder.setSocketTimeout((int) openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout().toMillis());
            }

            return requestConfigBuilder;
        });
    }
}
