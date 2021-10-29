/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ConnectionConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);

  private static final String SERVICE_NAME = "es";
  private static final String DEFAULT_AWS_REGION = "us-east-1";

  public static final String HOSTS = "hosts";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String SOCKET_TIMEOUT = "socket_timeout";
  public static final String CONNECT_TIMEOUT = "connect_timeout";
  public static final String CERT_PATH = "cert";
  public static final String INSECURE = "insecure";
  public static final String AWS_SIGV4 = "aws_sigv4";
  public static final String AWS_REGION = "aws_region";
  public static final String AWS_STS_ROLE_ARN = "aws_sts_role_arn";
  public static final String PROXY = "proxy";

  /**
   * The valid port range per https://tools.ietf.org/html/rfc6335.
   */
  private final static ValueRange VALID_PORT_RANGE = ValueRange.of(0, 65535);


  private final List<String> hosts;
  private final String username;
  private final String password;
  private final Path certPath;
  private final Integer socketTimeout;
  private final Integer connectTimeout;
  private final boolean insecure;
  private final boolean awsSigv4;
  private final String awsRegion;
  private final String awsStsRoleArn;
  private final Optional<String> proxy;
  private final String pipelineName;

  List<String> getHosts() {
    return hosts;
  }

  String getUsername() {
    return username;
  }

  String getPassword() {
    return password;
  }

  boolean isAwsSigv4() {
    return awsSigv4;
  }

  String getAwsRegion() {
    return awsRegion;
  }

  String getAwsStsRoleArn() {
    return awsStsRoleArn;
  }

  Path getCertPath() {
    return certPath;
  }

  Optional<String> getProxy() {
    return proxy;
  }

  Integer getSocketTimeout() {
    return socketTimeout;
  }

  Integer getConnectTimeout() {
    return connectTimeout;
  }

  private ConnectionConfiguration(final Builder builder) {
    this.hosts = builder.hosts;
    this.username = builder.username;
    this.password = builder.password;
    this.socketTimeout = builder.socketTimeout;
    this.connectTimeout = builder.connectTimeout;
    this.certPath = builder.certPath;
    this.insecure = builder.insecure;
    this.awsSigv4 = builder.awsSigv4;
    this.awsRegion = builder.awsRegion;
    this.awsStsRoleArn = builder.awsStsRoleArn;
    this.proxy = builder.proxy;
    this.pipelineName = builder.pipelineName;
  }

  public static ConnectionConfiguration readConnectionConfiguration(final PluginSetting pluginSetting){
    @SuppressWarnings("unchecked")
    final List<String> hosts = (List<String>) pluginSetting.getAttributeFromSettings(HOSTS);
    ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(hosts);
    final String username = (String) pluginSetting.getAttributeFromSettings(USERNAME);
    builder.withPipelineName(pluginSetting.getPipelineName());
    if (username != null) {
      builder = builder.withUsername(username);
    }
    final String password = (String) pluginSetting.getAttributeFromSettings(PASSWORD);
    if (password != null) {
      builder = builder.withPassword(password);
    }
    final Integer socketTimeout = (Integer) pluginSetting.getAttributeFromSettings(SOCKET_TIMEOUT);
    if (socketTimeout != null) {
      builder = builder.withSocketTimeout(socketTimeout);
    }
    final Integer connectTimeout = (Integer) pluginSetting.getAttributeFromSettings(CONNECT_TIMEOUT);
    if (connectTimeout != null) {
      builder = builder.withConnectTimeout(connectTimeout);
    }

    builder.withAwsSigv4(pluginSetting.getBooleanOrDefault(AWS_SIGV4, false));
    if (builder.awsSigv4) {
      builder.withAwsRegion(pluginSetting.getStringOrDefault(AWS_REGION, DEFAULT_AWS_REGION));
      builder.withAWSStsRoleArn(pluginSetting.getStringOrDefault(AWS_STS_ROLE_ARN, null));
    }

    final String certPath = pluginSetting.getStringOrDefault(CERT_PATH, null);
    final boolean insecure = pluginSetting.getBooleanOrDefault(INSECURE, false);
    if (certPath != null) {
      builder = builder.withCert(certPath);
    } else {
      //We will set insecure flag only if certPath is null
      builder = builder.withInsecure(insecure);
    }
    final String proxy = pluginSetting.getStringOrDefault(PROXY, null);
    builder = builder.withProxy(proxy);

    return builder.build();
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public RestHighLevelClient createClient() {
    final HttpHost[] httpHosts = new HttpHost[hosts.size()];
    int i = 0;
    for (final String host : hosts) {
      httpHosts[i] = HttpHost.create(host);
      i++;
    }
    final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
    /*
     * Given that this is a patch release, we will support only the IAM based access policy AES domains.
     * We will not support FGAC and Custom endpoint domains. This will be followed in the next version.
     */
    if(awsSigv4) {
      attachSigV4(restClientBuilder);
    } else {
      attachUserCredentials(restClientBuilder);
    }
    restClientBuilder.setRequestConfigCallback(
            requestConfigBuilder -> {
              if (connectTimeout != null) {
                requestConfigBuilder.setConnectTimeout(connectTimeout);
              }
              if (socketTimeout != null) {
                requestConfigBuilder.setSocketTimeout(socketTimeout);
              }
              return requestConfigBuilder;
            });
    return new RestHighLevelClient(restClientBuilder);
  }

  private void attachSigV4(final RestClientBuilder restClientBuilder) {
    //if aws signing is enabled we will add AWSRequestSigningApacheInterceptor interceptor,
    //if not follow regular credentials process
    LOG.info("{} is set, will sign requests using AWSRequestSigningApacheInterceptor", AWS_SIGV4);
    final Aws4Signer aws4Signer = Aws4Signer.create();
    AwsCredentialsProvider credentialsProvider;
    if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
      credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
              .stsClient(StsClient.create())
              .refreshRequest(AssumeRoleRequest.builder()
                      .roleSessionName("OpenSearch-Sink-" + UUID.randomUUID()
                              .toString())
                      .roleArn(awsStsRoleArn)
                      .build())
              .build();
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }
    final HttpRequestInterceptor httpRequestInterceptor = new AwsRequestSigningApacheInterceptor(SERVICE_NAME, aws4Signer,
            credentialsProvider, awsRegion);
    restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
      httpClientBuilder.addInterceptorLast(httpRequestInterceptor);
      attachSSLContext(httpClientBuilder);
      setHttpProxyIfApplicable(httpClientBuilder);
      return httpClientBuilder;
    });
  }

  private void attachUserCredentials(final RestClientBuilder restClientBuilder) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (username != null) {
      LOG.info("Using the username provided in the config.");
      credentialsProvider.setCredentials(
              AuthScope.ANY, new UsernamePasswordCredentials(username, password));
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
    proxy.ifPresent(
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
    final SSLContext sslContext = certPath != null ? getCAStrategy(certPath) : getTrustAllStrategy();
    httpClientBuilder.setSSLContext(sslContext);
    if (this.insecure) {
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

  public static class Builder {
    private final List<String> hosts;
    private String username;
    private String password;
    private Integer socketTimeout;
    private Integer connectTimeout;
    private Path certPath;
    private boolean insecure;
    private boolean awsSigv4;
    private String awsRegion;
    private String awsStsRoleArn;
    private Optional<String> proxy = Optional.empty();
    private String pipelineName;


    public Builder(final List<String> hosts) {
      checkArgument(hosts != null, "hosts cannot be null");
      checkArgument(hosts.size() > 0, "hosts cannot be empty list");
      this.hosts = hosts;
    }

    public Builder withUsername(final String username) {
      checkArgument(username != null, "username cannot be null");
      this.username = username;
      return this;
    }

    public Builder withPassword(final String password) {
      checkArgument(password != null, "password cannot be null");
      this.password = password;
      return this;
    }

    public Builder withSocketTimeout(final Integer socketTimeout) {
      checkArgument(socketTimeout != null, "socketTimeout cannot be null");
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder withConnectTimeout(final Integer connectTimeout) {
      checkArgument(connectTimeout != null, "connectTimeout cannot be null");
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder withCert(final String certPath) {
      checkArgument(certPath != null, "cert cannot be null");
      this.certPath = Paths.get(certPath);
      return this;
    }

    public Builder withInsecure(final boolean insecure) {
      this.insecure = insecure;
      return this;
    }

    public Builder withAwsSigv4(final boolean awsSigv4) {
      this.awsSigv4 = awsSigv4;
      return this;
    }

    public Builder withAwsRegion(final String awsRegion) {
      checkNotNull(awsRegion, "awsRegion cannot be null");
      this.awsRegion = awsRegion;
      return this;
    }

    public Builder withAWSStsRoleArn(final String awsStsRoleArn) {
      checkArgument(awsStsRoleArn == null || awsStsRoleArn.length() <= 2048, "awsStsRoleArn length cannot exceed 2048");
      if(awsStsRoleArn != null) {
        try {
          Arn.fromString(awsStsRoleArn);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid ARN format for awsStsRoleArn");
        }
      }
      this.awsStsRoleArn = awsStsRoleArn;
      return this;
    }

    public Builder withProxy(final String proxy) {
      this.proxy = Optional.ofNullable(proxy);
      return this;
    }

    public Builder withPipelineName(final String pipelineName) {
      this.pipelineName = pipelineName;
      return this;
    }

    public ConnectionConfiguration build() {
      return new ConnectionConfiguration(this);
    }
  }
}
