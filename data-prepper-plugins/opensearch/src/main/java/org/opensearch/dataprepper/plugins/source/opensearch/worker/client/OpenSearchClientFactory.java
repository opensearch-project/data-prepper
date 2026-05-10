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
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
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
import org.opensearch.dataprepper.aws.api.AwsRequestSigningApache4Interceptor;
import org.opensearch.dataprepper.plugins.source.opensearch.AuthConfig;
import org.opensearch.dataprepper.plugins.certificate.validation.PemObjectValidator;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.nio.file.Path;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Objects;

public class OpenSearchClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchClientFactory.class);

    private static final String AOS_SERVICE_NAME = "es";
    private static final String AOSS_SERVICE_NAME = "aoss";
    /**
     * The valid port range per https://tools.ietf.org/html/rfc6335.
     */
    private static final ValueRange VALID_PORT_RANGE = ValueRange.of(0, 65535);

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

        return new AwsSdk2Transport(createSdkAsyncHttpClient(openSearchSourceConfiguration),
                HttpHost.create(openSearchSourceConfiguration.getHosts().get(0)).getHostName(),
                isServerlessCollection ? AOSS_SERVICE_NAME : AOS_SERVICE_NAME,
                openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion(),
                AwsSdk2TransportOptions.builder()
                        .setCredentials(awsCredentialsProvider)
                        .setMapper(new JacksonJsonpMapper())
                        .build());

    }

    public SdkAsyncHttpClient createSdkAsyncHttpClient(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        if (Objects.nonNull(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout())) {
            builder.connectionTimeout(openSearchSourceConfiguration.getConnectionConfiguration().getConnectTimeout());
        }

        attachSSLContext(builder, openSearchSourceConfiguration);

        return builder.build();
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
        final HttpRequestInterceptor httpRequestInterceptor = new AwsRequestSigningApache4Interceptor(AOS_SERVICE_NAME, aws4Signer,
                awsCredentialsProvider, openSearchSourceConfiguration.getAwsAuthenticationOptions().getAwsRegion());
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.addInterceptorLast(httpRequestInterceptor);
            attachSSLContext(httpClientBuilder, openSearchSourceConfiguration);
            setHttpProxyIfApplicable(httpClientBuilder, openSearchSourceConfiguration);
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
            setHttpProxyIfApplicable(httpClientBuilder, openSearchSourceConfiguration);
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
            setHttpProxyIfApplicable(httpClientBuilder, openSearchSourceConfiguration);
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
        final String username;
        final String password;
        final AuthConfig authConfig = openSearchSourceConfiguration.getAuthConfig();
        if (authConfig != null) {
            username = openSearchSourceConfiguration.getAuthConfig().getUsername();
            password = openSearchSourceConfiguration.getAuthConfig().getPassword();
        } else {
            username = openSearchSourceConfiguration.getUsername();
            password = openSearchSourceConfiguration.getPassword();
        }
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
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

    private void attachSSLContext(final NettyNioAsyncHttpClient.Builder asyncClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        TrustManager[] trustManagers = createTrustManagers(openSearchSourceConfiguration.getConnectionConfiguration());
        if (trustManagers.length > 0) {
            asyncClientBuilder.tlsTrustManagersProvider(() -> trustManagers);
        }
    }

    private void attachSSLContext(final HttpAsyncClientBuilder httpClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

        final ConnectionConfiguration connectionConfiguration = openSearchSourceConfiguration.getConnectionConfiguration();
        final SSLContext sslContext = getCAStrategy(connectionConfiguration);
        httpClientBuilder.setSSLContext(sslContext);

        if (connectionConfiguration.isInsecure()) {
            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
    }

    private TrustManager[] createTrustManagers(final ConnectionConfiguration connectionConfiguration) {
        final Path certPath = connectionConfiguration.getCertPath();
        final String certificate = connectionConfiguration.getCertificate();
        if (certPath != null) {
            return TrustStoreProvider.createTrustManager(certPath);
        } else if (certificate != null) {
            if (PemObjectValidator.isPemObject(certificate)) {
                return TrustStoreProvider.createTrustManager(certificate);
            } else {
                return TrustStoreProvider.createTrustManager(Path.of(certificate));}
        } else if (connectionConfiguration.isInsecure()) {
            return TrustStoreProvider.createTrustAllManager();

        } else {
            return new TrustManager[0];
        }
    }

    private SSLContext getCAStrategy(final ConnectionConfiguration connectionConfiguration) {
        final Path certPath = connectionConfiguration.getCertPath();
        final String certificate = connectionConfiguration.getCertificate();
        if (certPath != null) {
            return TrustStoreProvider.createSSLContext(certPath);
        } else if (certificate != null) {
            if (PemObjectValidator.isPemObject(certificate)) {
                return TrustStoreProvider.createSSLContext(certificate);
            } else {
                return TrustStoreProvider.createSSLContext(Path.of(connectionConfiguration.getCertificate()));
            }
        } else if (connectionConfiguration.isInsecure()) {
                return TrustStoreProvider.createSSLContextWithTrustAllStrategy();
        } else {
            return null;
        }
    }

    private void setHttpProxyIfApplicable(final HttpAsyncClientBuilder httpClientBuilder, final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        final String proxy = openSearchSourceConfiguration.getConnectionConfiguration().getProxy();
        if (proxy != null) {
            final HttpHost httpProxyHost = HttpHost.create(proxy);
            checkProxyPort(httpProxyHost.getPort());
            httpClientBuilder.setProxy(httpProxyHost);
        }
      }
    
    private void checkProxyPort(final int port) {
        if (!VALID_PORT_RANGE.isValidIntValue(port)) {
          throw new IllegalArgumentException("Invalid or missing proxy port.");
        }
    }
}
