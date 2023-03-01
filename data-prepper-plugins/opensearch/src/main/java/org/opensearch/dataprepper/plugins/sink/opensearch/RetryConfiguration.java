/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.configuration.PluginSetting;

import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";
  public static final String MAX_RETRIES = "max_retries";

  private final String dlqFile;
  private final int maxRetries;

  public String getDlqFile() {
    return dlqFile;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public static class Builder {
    private String dlqFile;
    private int maxRetries = Integer.MAX_VALUE;

    public Builder withDlqFile(final String dlqFile) {
      checkNotNull(dlqFile, "dlqFile cannot be null.");
      this.dlqFile = dlqFile;
      return this;
    }

    public Builder withMaxRetries(final Integer maxRetries) {
      checkNotNull(maxRetries, "maxRetries cannot be null.");
      this.maxRetries = maxRetries;
      return this;
    }
    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(final Builder builder) {
    this.dlqFile = builder.dlqFile;
    this.maxRetries = builder.maxRetries;
  }

  public static RetryConfiguration readRetryConfig(final PluginSetting pluginSetting) {
    RetryConfiguration.Builder builder = new RetryConfiguration.Builder();
    final String dlqFile = (String) pluginSetting.getAttributeFromSettings(DLQ_FILE);
    if (dlqFile != null) {
      builder = builder.withDlqFile(dlqFile);
    }
    final Integer maxRetries = (Integer) pluginSetting.getAttributeFromSettings(MAX_RETRIES);
    if (maxRetries != null) {
      builder = builder.withMaxRetries(maxRetries);
    }
    return builder.build();
  }
}
