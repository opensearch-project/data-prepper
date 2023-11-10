/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsRequestSigningApache4Interceptor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DISTRIBUTION_VERSION;

public class ConnectionConfiguration {
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);
  private static final String AWS_IAM_ROLE = "role";
  private static final String AWS_IAM = "iam";
  private static final String AOS_SERVICE_NAME = "es";
  private static final String AOSS_SERVICE_NAME = "aoss";
  private static final String DEFAULT_AWS_REGION = "us-east-1";

  public static final String HOSTS = "hosts";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String SOCKET_TIMEOUT = "socket_timeout";
  public static final String CONNECT_TIMEOUT = "connect_timeout";
  public static final String CERT_PATH = "cert";
  public static final String INSECURE = "insecure";
  public static final String AWS_OPTION = "aws";
  public static final String AWS_SIGV4 = "aws_sigv4";
  public static final String AWS_REGION = "aws_region";
  public static final String AWS_STS_ROLE_ARN = "aws_sts_role_arn";
  public static final String AWS_STS_EXTERNAL_ID = "aws_sts_external_id";
  public static final String AWS_STS_HEADER_OVERRIDES = "aws_sts_header_overrides";
  public static final String PROXY = "proxy";
  public static final String SERVERLESS = "serverless";
  public static final String SERVERLESS_OPTIONS = "serverless_options";
  public static final String COLLECTION_NAME = "collection_name";
  public static final String NETWORK_POLICY_NAME = "network_policy_name";
  public static final String VPCE_ID = "vpce_id";
  public static final String REQUEST_COMPRESSION_ENABLED = "enable_request_compression";

  /**
   * The valid port range per https://tools.ietf.org/html/rfc6335.
   */
  private static final ValueRange VALID_PORT_RANGE = ValueRange.of(0, 65535);


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
  private final String awsStsExternalId;
  private final Map<String, String> awsStsHeaderOverrides;
  private final Optional<String> proxy;
  private final String pipelineName;
  private final boolean serverless;
  private final String serverlessNetworkPolicyName;
  private final String serverlessCollectionName;
  private final String serverlessVpceId;
  private final boolean requestCompressionEnabled;

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

  public boolean isServerless() {
    return serverless;
  }

  public String getServerlessNetworkPolicyName() {
    return serverlessNetworkPolicyName;
  }

  public String getServerlessCollectionName() {
    return serverlessCollectionName;
  }

  public String getServerlessVpceId() {
    return serverlessVpceId;
  }

  boolean isRequestCompressionEnabled() {
    return requestCompressionEnabled;
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
    this.awsStsExternalId = builder.awsStsExternalId;
    this.awsStsHeaderOverrides = builder.awsStsHeaderOverrides;
    this.proxy = builder.proxy;
    this.serverless = builder.serverless;
    this.serverlessNetworkPolicyName = builder.serverlessNetworkPolicyName;
    this.serverlessCollectionName = builder.serverlessCollectionName;
    this.serverlessVpceId = builder.serverlessVpceId;
    this.requestCompressionEnabled = builder.requestCompressionEnabled;
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
    final Integer socketTimeout = pluginSetting.getIntegerOrDefault(SOCKET_TIMEOUT, null);
    if (socketTimeout != null) {
      builder = builder.withSocketTimeout(socketTimeout);
    }
    final Integer connectTimeout = pluginSetting.getIntegerOrDefault(CONNECT_TIMEOUT, null);
    if (connectTimeout != null) {
      builder = builder.withConnectTimeout(connectTimeout);
    }

    Map<String, Object> awsOption = pluginSetting.getTypedMap(AWS_OPTION, String.class, Object.class);
    boolean awsOptionUsed = false;
    builder.withAwsSigv4(false);
    if (awsOption != null && !awsOption.isEmpty()) {
        awsOptionUsed = true;
        builder.withAwsSigv4(true);
        builder.withAwsRegion((String)(awsOption.getOrDefault(AWS_REGION.substring(4), DEFAULT_AWS_REGION)));
        builder.withAWSStsRoleArn((String)(awsOption.getOrDefault(AWS_STS_ROLE_ARN.substring(4), null)));
        builder.withAWSStsExternalId((String)(awsOption.getOrDefault(AWS_STS_EXTERNAL_ID.substring(4), null)));
        builder.withAwsStsHeaderOverrides((Map<String, String>)awsOption.get(AWS_STS_HEADER_OVERRIDES.substring(4)));
        builder.withServerless(OBJECT_MAPPER.convertValue(
                awsOption.getOrDefault(SERVERLESS, false), Boolean.class));

        Map<String, String> serverlessOptions = (Map<String, String>) awsOption.get(SERVERLESS_OPTIONS);
        if (serverlessOptions != null && !serverlessOptions.isEmpty()) {
          builder.withServerlessNetworkPolicyName((String)(serverlessOptions.getOrDefault(NETWORK_POLICY_NAME, null)));
          builder.withServerlessCollectionName((String)(serverlessOptions.getOrDefault(COLLECTION_NAME, null)));
          builder.withServerlessVpceId((String)(serverlessOptions.getOrDefault(VPCE_ID, null)));
        }
    } else {
      builder.withServerless(false);
    }
    boolean awsSigv4 = pluginSetting.getBooleanOrDefault(AWS_SIGV4, false);
    final String awsOptionConflictMessage = String.format("%s option cannot be used along with %s option", AWS_SIGV4, AWS_OPTION);
    if (awsSigv4) {
      if (awsOptionUsed) {
        throw new RuntimeException(awsOptionConflictMessage);
      }
      builder.withAwsSigv4(true);
      builder.withAwsRegion(pluginSetting.getStringOrDefault(AWS_REGION, DEFAULT_AWS_REGION));
      builder.withAWSStsRoleArn(pluginSetting.getStringOrDefault(AWS_STS_ROLE_ARN, null));
      builder.withAWSStsExternalId(pluginSetting.getStringOrDefault(AWS_STS_EXTERNAL_ID, null));
      builder.withAwsStsHeaderOverrides(pluginSetting.getTypedMap(AWS_STS_HEADER_OVERRIDES, String.class, String.class));
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

    final String distributionVersionName = pluginSetting.getStringOrDefault(DISTRIBUTION_VERSION,
            DistributionVersion.DEFAULT.getVersion());
    final DistributionVersion distributionVersion = DistributionVersion.fromTypeName(distributionVersionName);
    final boolean requestCompressionEnabled = pluginSetting.getBooleanOrDefault(
            REQUEST_COMPRESSION_ENABLED, !DistributionVersion.ES6.equals(distributionVersion));
    builder = builder.withRequestCompressionEnabled(requestCompressionEnabled);

    return builder.build();
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public RestHighLevelClient createClient(AwsCredentialsSupplier awsCredentialsSupplier) {
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
      attachSigV4(restClientBuilder, awsCredentialsSupplier);
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

  private void attachSigV4(final RestClientBuilder restClientBuilder, AwsCredentialsSupplier awsCredentialsSupplier) {
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
    final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
        .withStsRoleArn(awsStsRoleArn)
        .withStsExternalId(awsStsExternalId)
        .withRegion(awsRegion)
        .withStsHeaderOverrides(awsStsHeaderOverrides)
        .build();
    return awsCredentialsOptions;
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

  public OpenSearchClient createOpenSearchClient(final RestHighLevelClient restHighLevelClient, final AwsCredentialsSupplier awsCredentialsSupplier) {
    return new OpenSearchClient(createOpenSearchTransport(restHighLevelClient, awsCredentialsSupplier));
  }

  private OpenSearchTransport createOpenSearchTransport(final RestHighLevelClient restHighLevelClient, final AwsCredentialsSupplier awsCredentialsSupplier) {
    if (awsSigv4) {
      final AwsCredentialsOptions awsCredentialsOptions = createAwsCredentialsOptions();
      final AwsCredentialsProvider credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
      final String serviceName = serverless ? AOSS_SERVICE_NAME : AOS_SERVICE_NAME;

      final AwsSdk2TransportOptions.Builder transportOptions = AwsSdk2TransportOptions.builder()
              .setCredentials(credentialsProvider)
              .setMapper(new PreSerializedJsonpMapper());

      if (!isRequestCompressionEnabled()) {
        // Disable compression for all requests
        transportOptions.setRequestCompressionSize(Integer.MAX_VALUE);
      }

      return new AwsSdk2Transport(createSdkHttpClient(), HttpHost.create(hosts.get(0)).getHostName(),
              serviceName, Region.of(awsRegion), transportOptions.build());
    } else {
      return new RestClientTransport(
              restHighLevelClient.getLowLevelClient(), new PreSerializedJsonpMapper());
    }
  }

  private SdkHttpClient createSdkHttpClient() {
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    if (connectTimeout != null) {
      apacheHttpClientBuilder.connectionTimeout(Duration.ofMillis(connectTimeout));
    }
    if (socketTimeout != null) {
      apacheHttpClientBuilder.socketTimeout(Duration.ofMillis(socketTimeout));
    }
    attachSSLContext(apacheHttpClientBuilder);
    setHttpProxyIfApplicable(apacheHttpClientBuilder);
    return apacheHttpClientBuilder.build();
  }

  private void attachSSLContext(final ApacheHttpClient.Builder apacheHttpClientBuilder) {
    TrustManager[] trustManagers = createTrustManagers(certPath);
    apacheHttpClientBuilder.tlsTrustManagersProvider(() -> trustManagers);
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

  private void setHttpProxyIfApplicable(final ApacheHttpClient.Builder apacheHttpClientBuilder) {
    proxy.ifPresent(
            p -> {
              final URI endpoint = URI.create(p);
              final ProxyConfiguration proxyConfiguration = ProxyConfiguration.builder()
                      .endpoint(endpoint)
                      .build();
              checkProxyPort(endpoint.getPort());
              apacheHttpClientBuilder.proxyConfiguration(proxyConfiguration);
            }
    );
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
    private String awsStsExternalId;
    private Map<String, String> awsStsHeaderOverrides;
    private Optional<String> proxy = Optional.empty();
    private String pipelineName;
    private boolean serverless;
    private String serverlessNetworkPolicyName;
    private String serverlessCollectionName;
    private String serverlessVpceId;
    private boolean requestCompressionEnabled;

    private void validateStsRoleArn(final String awsStsRoleArn) {
      final Arn arn = getArn(awsStsRoleArn);
      if (!AWS_IAM.equals(arn.service())) {
        throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
      }
      final Optional<String> resourceType = arn.resource().resourceType();
      if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
        throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
      }
    }

    private Arn getArn(final String awsStsRoleArn) {
      try {
        return Arn.fromString(awsStsRoleArn);
      } catch (final Exception e) {
        throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", awsStsRoleArn));
      }
    }

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
      this.certPath = new File(certPath).toPath();
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
        validateStsRoleArn(awsStsRoleArn);
      }
      this.awsStsRoleArn = awsStsRoleArn;
      return this;
    }

    public Builder withAWSStsExternalId(final String awsStsExternalId) {
      checkArgument(awsStsExternalId == null || awsStsExternalId.length() <= 1224, "awsStsExternalId length cannot exceed 1224");
      this.awsStsExternalId = awsStsExternalId;
      return this;
    }

    public Builder withAwsStsHeaderOverrides(final Map<String, String> headerOverrides) {
      if(headerOverrides != null && !headerOverrides.isEmpty()) {
        this.awsStsHeaderOverrides = headerOverrides;
      }
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

    public Builder withServerless(boolean serverless) {
      this.serverless = serverless;
      return this;
    }

    public Builder withServerlessNetworkPolicyName(final String serverlessNetworkPolicyName) {
      this.serverlessNetworkPolicyName = serverlessNetworkPolicyName;
      return this;
    }

    public Builder withServerlessCollectionName(final String serverlessCollectionName) {
      this.serverlessCollectionName = serverlessCollectionName;
      return this;
    }

    public Builder withServerlessVpceId(final String serverlessVpceId) {
      this.serverlessVpceId = serverlessVpceId;
      return this;
    }

    public Builder withRequestCompressionEnabled(final boolean requestCompressionEnabled) {
      this.requestCompressionEnabled = requestCompressionEnabled;
      return this;
    }

    public ConnectionConfiguration build() {
      return new ConnectionConfiguration(this);
    }
  }
}
