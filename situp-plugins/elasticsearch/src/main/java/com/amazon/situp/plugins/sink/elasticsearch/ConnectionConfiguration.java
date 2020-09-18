package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ConnectionConfiguration {
  public static final String HOSTS = "hosts";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String SOCKET_TIMEOUT = "socket_timeout";
  public static final String CONNECT_TIMEOUT = "connect_timeout";

  private final List<String> hosts;
  private final String username;
  private final String password;
  private final Integer socketTimeout;
  private final Integer connectTimeout;

  public List<String> getHosts() {
    return hosts;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public static class Builder {
    private List<String> hosts;
    private String username;
    private String password;
    private Integer socketTimeout;
    private Integer connectTimeout;

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

    public ConnectionConfiguration build() {
      return new ConnectionConfiguration(this);
    }
  }

  private ConnectionConfiguration(final Builder builder) {
    this.hosts = builder.hosts;
    this.username = builder.username;
    this.password = builder.password;
    this.socketTimeout = builder.socketTimeout;
    this.connectTimeout = builder.connectTimeout;
  }

  public static ConnectionConfiguration readConnectionConfiguration(final PluginSetting pluginSetting){
    @SuppressWarnings("unchecked")
    final List<String> hosts = (List<String>) pluginSetting.getAttributeFromSettings(HOSTS);
    ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(hosts);
    final String username = (String) pluginSetting.getAttributeFromSettings(USERNAME);
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

    return builder.build();
  }

  public RestHighLevelClient createClient() {
    final HttpHost[] httpHosts = new HttpHost[hosts.size()];
    int i = 0;
    for (final String host : hosts) {
      httpHosts[i] = HttpHost.create(host);
      i++;
    }
    final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
    if (username != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
              AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      restClientBuilder.setHttpClientConfigCallback(
              httpAsyncClientBuilder ->
                      httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
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
}
