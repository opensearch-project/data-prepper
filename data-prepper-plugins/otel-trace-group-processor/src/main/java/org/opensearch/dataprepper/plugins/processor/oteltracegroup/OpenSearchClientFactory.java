package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsRequestSigningApache4Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration.AOS_SERVICE_NAME;
import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration.AWS_SIGV4;
import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration.VALID_PORT_RANGE;

public class OpenSearchClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchClientFactory.class);

    private final ConnectionConfiguration connectionConfiguration;

    public static OpenSearchClientFactory fromConnectionConfiguration(
            final ConnectionConfiguration connectionConfiguration) {
        return new OpenSearchClientFactory(connectionConfiguration);
    }

    OpenSearchClientFactory(final ConnectionConfiguration connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
    }

    public RestHighLevelClient createRestHighLevelClient(final AwsCredentialsSupplier awsCredentialsSupplier) {
        final HttpHost[] httpHosts = new HttpHost[connectionConfiguration.getHosts().size()];
        int i = 0;
        for (final String host : connectionConfiguration.getHosts()) {
            httpHosts[i] = HttpHost.create(host);
            i++;
        }
        final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        /*
         * Given that this is a patch release, we will support only the IAM based access policy AES domains.
         * We will not support FGAC and Custom endpoint domains. This will be followed in the next version.
         */
        if (connectionConfiguration.isAwsSigv4()) {
            attachSigV4(restClientBuilder, awsCredentialsSupplier);
        } else {
            attachUserCredentials(restClientBuilder);
        }
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder -> {
                    if (connectionConfiguration.getConnectTimeout() != null) {
                        requestConfigBuilder.setConnectTimeout(connectionConfiguration.getConnectTimeout());
                    }
                    if (connectionConfiguration.getSocketTimeout() != null) {
                        requestConfigBuilder.setSocketTimeout(connectionConfiguration.getSocketTimeout());
                    }
                    return requestConfigBuilder;
                });
        return new RestHighLevelClient(restClientBuilder);
    }

    private void attachSigV4(final RestClientBuilder restClientBuilder, AwsCredentialsSupplier awsCredentialsSupplier) {
        final String awsRegion = connectionConfiguration.getAwsOption() == null ?
                connectionConfiguration.getAwsRegion() : connectionConfiguration.getAwsOption().getRegion();
        //if aws signing is enabled we will add AWSRequestSigningApacheInterceptor interceptor,
        //if not follow regular credentials process
        LOG.info("{} is set, will sign requests using AWSRequestSigningApacheInterceptor", AWS_SIGV4);
        final Aws4Signer aws4Signer = Aws4Signer.create();
        final AwsCredentialsOptions awsCredentialsOptions = createAwsCredentialsOptions();
        final AwsCredentialsProvider credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        final HttpRequestInterceptor httpRequestInterceptor = new AwsRequestSigningApache4Interceptor(AOS_SERVICE_NAME, aws4Signer,
                credentialsProvider, awsRegion);
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.addInterceptorLast(httpRequestInterceptor);
            attachSSLContext(httpClientBuilder);
            setHttpProxyIfApplicable(httpClientBuilder);
            return httpClientBuilder;
        });
    }

    public AwsCredentialsOptions createAwsCredentialsOptions() {
        final String awsStsRoleArn = connectionConfiguration.getAwsOption() == null ?
                connectionConfiguration.getAwsStsRoleArn() : connectionConfiguration.getAwsOption().getStsRoleArn();
        final String awsStsExternalId = connectionConfiguration.getAwsOption() == null ?
                connectionConfiguration.getAwsStsExternalId() :
                connectionConfiguration.getAwsOption().getStsExternalId();
        final String awsRegion = connectionConfiguration.getAwsOption() == null ?
                connectionConfiguration.getAwsRegion() : connectionConfiguration.getAwsOption().getRegion();
        final Map<String, String> awsStsHeaderOverrides = connectionConfiguration.getAwsOption() == null ?
                connectionConfiguration.getAwsStsHeaderOverrides() :
                connectionConfiguration.getAwsOption().getStsHeaderOverrides();
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsStsRoleArn)
                .withStsExternalId(awsStsExternalId)
                .withRegion(awsRegion)
                .withStsHeaderOverrides(awsStsHeaderOverrides)
                .build();
        return awsCredentialsOptions;
    }

    private void attachUserCredentials(final RestClientBuilder restClientBuilder) {
        final AuthConfig authConfig = connectionConfiguration.getAuthConfig();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (authConfig != null) {
            if (authConfig.getUsername() != null) {
                LOG.info("Using the authentication provided in the config.");
                credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(authConfig.getUsername(), authConfig.getPassword()));
            }
        } else {
            final String username = connectionConfiguration.getUsername();
            final String password = connectionConfiguration.getPassword();
            if (username != null) {
                LOG.info("Using the username provided in the config.");
                credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            }
        }
        restClientBuilder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    attachSSLContext(httpClientBuilder);
                    setHttpProxyIfApplicable(httpClientBuilder);
                    return httpClientBuilder;
                }
        );
    }

    private void setHttpProxyIfApplicable(final HttpAsyncClientBuilder httpClientBuilder) {
        Optional.ofNullable(connectionConfiguration.getProxy()).ifPresent(
                p -> {
                    final HttpHost httpProxyHost = HttpHost.create(p);
                    checkProxyPort(httpProxyHost.getPort());
                    httpClientBuilder.setProxy(httpProxyHost);
                }
        );
    }

    private void checkProxyPort(final int port) {
        if (!VALID_PORT_RANGE.isValidIntValue(port)) {
            throw new IllegalArgumentException("Invalid or missing proxy port.");
        }
    }

    private void attachSSLContext(final HttpAsyncClientBuilder httpClientBuilder) {
        final Path certPath = connectionConfiguration.getCertPath();
        final SSLContext sslContext = certPath != null ? getCAStrategy(certPath) : getTrustAllStrategy();
        httpClientBuilder.setSSLContext(sslContext);
        if (connectionConfiguration.isInsecure()) {
            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
    }

    private SSLContext getCAStrategy(Path certPath) {
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
