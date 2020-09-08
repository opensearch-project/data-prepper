package com.amazon.situp.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class ElasticsearchSinkConfiguration {
  /**
   * TODO: add retryConfiguration
   */
  private final ConnectionConfiguration connectionConfiguration;
  private final IndexConfiguration indexConfiguration;

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public IndexConfiguration getIndexConfiguration() {
    return indexConfiguration;
  }

  public static class Builder {
    private ConnectionConfiguration connectionConfiguration;
    private IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();

    public Builder(final ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration cannot be null");
      this.connectionConfiguration = connectionConfiguration;
    }

    public Builder withIndexConfiguration(final IndexConfiguration indexConfiguration) {
      checkArgument(indexConfiguration != null, "indexConfiguration cannot be null");
      this.indexConfiguration = indexConfiguration;
      return this;
    }

    public ElasticsearchSinkConfiguration build() {
      return new ElasticsearchSinkConfiguration(this);
    }
  }

  private ElasticsearchSinkConfiguration(final Builder builder) {
    this.connectionConfiguration = builder.connectionConfiguration;
    this.indexConfiguration = builder.indexConfiguration;
  }
}
