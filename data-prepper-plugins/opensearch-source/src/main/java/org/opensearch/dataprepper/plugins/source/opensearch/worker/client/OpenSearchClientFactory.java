/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
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
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Objects;

public class OpenSearchClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchClientFactory.class);

    private static final String AOS_SERVICE_NAME = "es";
    private static final String AOSS_SERVICE_NAME = "aoss";

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    public static OpenSearchClientFactory create(final AwsCredentialsSupplier awsCredentialsSupplier) {
        return new OpenSearchClientFactory(awsCredentialsSupplier);
    }

    private OpenSearchClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    public OpenSearchClient provideOpenSearchClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        OpenSearchTransport transport;
        if (Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions())) {
            transport = createOpenSearchTransportForAws(openSearchSourceConfiguration);
        } else {
            final RestClient restClient = createOpenSearchRestClient(openSearchSourceConfiguration);
            transport = createOpenSearchTransport(restClient);
        }
        return new OpenSearchClient(transport);
    }

    public ElasticsearchClient provideElasticSearchClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final org.elasticsearch.client.RestClient restClientElasticsearch = createElasticSearchRestClient(openSearchSourceConfiguration);
        final ElasticsearchTransport elasticsearchTransport = createElasticSearchTransport(restClientElasticsearch);
        return new ElasticsearchClient(elasticsearchTransport);
    }

    private OpenSearchTransport createOpenSearchTransport(final RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    private OpenSearchTransport createOpenSearchTransportForAws(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                .withStsRoleArn(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .withStsExternalId(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId())
                .withStsHeaderOverrides(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build());

        final boolean isServerlessCollection = Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions()) &&
                openSearchSourceConfiguration.getAwsAuthenticationOptions().isServerlessCollection();

        return new AwsSdk2Transport(createSdkHttpClient(openSearchSourceConfiguration),
                HttpHost.create(openSearchSourceConfiguration.getHosts().get(0)).getHostName(),
                isServerlessCollection ? AOSS_SERVICE_NAME : AOS_SERVICE_NAME,
                openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion(),
                AwsSdk2TransportOptions.builder()
                        .setCredentials(awsCredentialsProvider)
                        .setMapper(new JacksonJsonpMapper())
                        .build());
    }

    private SdkHttpClient createSdkHttpClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();

        if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout())) {
            apacheHttpClientBuilder.connectionTimeout(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout());
        }

        if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout())) {
            apacheHttpClientBuilder.socketTimeout(openSearchSourceConfiguration.getConnectionConfiguration().getSocketTimeout());
        }

        attachSSLContext(apacheHttpClientBuilder, openSearchSourceConfiguration);

        return apacheHttpClientBuilder.build();
    }

    private RestClient createOpenSearchRestClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final List<String> hosts = openSearchSourceConfiguration.getHosts();
        final HttpHost[] httpHosts = new HttpHost[hosts.size()];

        int i = 0;
        for (final String host : hosts) {
            httpHosts[i] = HttpHost.create(host);
            i++;
        }

        final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);

        attachBasicAuth(restClientBuilder, openSearchSourceConfiguration);

        setConnectAndSocketTimeout(restClientBuilder, openSearchSourceConfiguration);

        return restClientBuilder.build();
    }

    private ElasticsearchTransport createElasticSearchTransport(final org.elasticsearch.client.RestClient restClient) {
        return new co.elastic.clients.transport.rest_client.RestClientTransport(restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper());
    }

    private org.elasticsearch.client.RestClient createElasticSearchRestClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
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

        if (Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions())) {
            attachSigV4ForElasticsearchClient(restClientBuilder, openSearchSourceConfiguration);
        } else {
            attachBasicAuth(restClientBuilder, openSearchSourceConfiguration);
        }
        setConnectAndSocketTimeout(restClientBuilder, openSearchSourceConfiguration);

        return restClientBuilder.build();
    }

    private void attachSigV4ForElasticsearchClient(final org.elasticsearch.client.RestClientBuilder restClientBuilder,
                                                   final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                .withStsRoleArn(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .withStsExternalId(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId())
                .withStsHeaderOverrides(openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build());
        final Aws4Signer aws4Signer = Aws4Signer.create();
        final HttpRequestInterceptor httpRequestInterceptor = new AwsRequestSigningApacheInterceptor(AOS_SERVICE_NAME, aws4Signer,
                awsCredentialsProvider, openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion());
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.addInterceptorLast(httpRequestInterceptor);
            attachSSLContext(httpClientBuilder, openSearchSourceConfiguration);
            httpClientBuilder.addInterceptorLast(
                    (HttpResponseInterceptor)
                            (response, context) ->
                                    response.addHeader("X-Elastic-Product", "Elasticsearch"));
            return httpClientBuilder;
        });
    }

    private void attachBasicAuth(final RestClientBuilder restClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            if (!openSearchSourceConfiguration.isAuthenticationDisabled()) {
                attachUsernameAndPassword(httpClientBuilder, openSearchSourceConfiguration);
            } else {
                LOG.warn("Authentication was explicitly disabled for the OpenSearch source");
            }

            attachSSLContext(httpClientBuilder, openSearchSourceConfiguration);
            return httpClientBuilder;
        });
    }

    private void attachBasicAuth(final org.elasticsearch.client.RestClientBuilder restClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {

            if (!openSearchSourceConfiguration.isAuthenticationDisabled()) {
                attachUsernameAndPassword(httpClientBuilder, openSearchSourceConfiguration);
            } else {
                LOG.warn("Authentication was explicitly disabled for the OpenSearch source");
            }
            attachSSLContext(httpClientBuilder, openSearchSourceConfiguration);
            httpClientBuilder.addInterceptorLast(
                    (HttpResponseInterceptor)
                            (response, context) ->
                                    response.addHeader("X-Elastic-Product", "Elasticsearch"));
            return httpClientBuilder;
        });
    }

    private void setConnectAndSocketTimeout(final RestClientBuilder restClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
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

    private void attachUsernameAndPassword(final HttpAsyncClientBuilder httpClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        LOG.info("Using username and password for auth for the OpenSearch source");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(openSearchSourceConfiguration.getUsername(), openSearchSourceConfiguration.getPassword()));
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }

    private void setConnectAndSocketTimeout(final org.elasticsearch.client.RestClientBuilder restClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
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

    private void attachSSLContext(final ApacheHttpClient.Builder apacheHttpClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        TrustManager[] trustManagers = createTrustManagers(openSearchSourceConfiguration.getConnectionConfiguration().getCertPath());
        apacheHttpClientBuilder.tlsTrustManagersProvider(() -> trustManagers);
    }

    private void attachSSLContext(final HttpAsyncClientBuilder httpClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

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
}
