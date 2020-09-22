package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchSinkConfiguration {
  /**
   * TODO: add retryConfiguration
   */
  private final ConnectionConfiguration connectionConfiguration;
  private final IndexConfiguration indexConfiguration;
  private final RetryConfiguration retryConfiguration;

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public IndexConfiguration getIndexConfiguration() {
    return indexConfiguration;
  }

  public RetryConfiguration getRetryConfiguration() {
    return retryConfiguration;
  }

  public static class Builder {
    private ConnectionConfiguration connectionConfiguration;
    private IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();
    private RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();

    public Builder(final ConnectionConfiguration connectionConfiguration) {
      checkNotNull(connectionConfiguration, "connectionConfiguration cannot be null");
      this.connectionConfiguration = connectionConfiguration;
    }

    public Builder withIndexConfiguration(final IndexConfiguration indexConfiguration) {
      checkNotNull(indexConfiguration, "indexConfiguration cannot be null");
      this.indexConfiguration = indexConfiguration;
      return this;
    }

    public Builder withRetryConfiguration(final RetryConfiguration retryConfiguration) {
      checkNotNull(retryConfiguration, "retryConfiguration cannot be null");
      this.retryConfiguration = retryConfiguration;
      return this;
    }

    public ElasticsearchSinkConfiguration build() {
      return new ElasticsearchSinkConfiguration(this);
    }
  }

  private ElasticsearchSinkConfiguration(final Builder builder) {
    this.connectionConfiguration = builder.connectionConfiguration;
    this.indexConfiguration = builder.indexConfiguration;
    this.retryConfiguration = builder.retryConfiguration;
  }

  public static ElasticsearchSinkConfiguration readESConfig(final PluginSetting pluginSetting) {
    final ConnectionConfiguration connectionConfiguration =
            ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
    final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(pluginSetting);

    return new ElasticsearchSinkConfiguration.Builder(connectionConfiguration)
            .withIndexConfiguration(indexConfiguration)
            .withRetryConfiguration(retryConfiguration)
            .build();
  }
}
