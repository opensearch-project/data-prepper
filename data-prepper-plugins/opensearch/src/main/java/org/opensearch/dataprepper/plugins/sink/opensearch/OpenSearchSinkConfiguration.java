/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

public class OpenSearchSinkConfiguration {

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

  private OpenSearchSinkConfiguration(
          final ConnectionConfiguration connectionConfiguration, final IndexConfiguration indexConfiguration,
          final RetryConfiguration retryConfiguration) {
    checkNotNull(connectionConfiguration, "connectionConfiguration cannot be null");
    checkNotNull(indexConfiguration, "indexConfiguration cannot be null");
    checkNotNull(retryConfiguration, "retryConfiguration cannot be null");
    this.connectionConfiguration = connectionConfiguration;
    this.indexConfiguration = indexConfiguration;
    this.retryConfiguration = retryConfiguration;
  }

  public static OpenSearchSinkConfiguration readOSConfig(final OpenSearchSinkConfig openSearchSinkConfig) {
    return readOSConfig(openSearchSinkConfig, null);
  }

  public static OpenSearchSinkConfiguration readOSConfig(final OpenSearchSinkConfig openSearchSinkConfig, final ExpressionEvaluator expressionEvaluator) {
    final ConnectionConfiguration connectionConfiguration =
            ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig, expressionEvaluator);
    final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(openSearchSinkConfig);

    return new OpenSearchSinkConfiguration(connectionConfiguration, indexConfiguration, retryConfiguration);
  }
}
