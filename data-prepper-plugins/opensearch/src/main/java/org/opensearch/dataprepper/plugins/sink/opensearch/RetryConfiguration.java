/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";
  public static final String MAX_RETRIES = "max_retries";
  public static final String DLQ = "dlq";

  private final String dlqFile;
  private final int maxRetries;
  private final PluginModel dlq;

  public String getDlqFile() {
    return dlqFile;
  }

  public Optional<PluginModel> getDlq() {
    return Optional.ofNullable(dlq);
  }

  public int getMaxRetries() {
    if (maxRetries < 1) {
        throw new IllegalArgumentException("max_retries must be > 1");
    }
    return maxRetries;
  }

  public static class Builder {
    private String dlqFile;
    private int maxRetries = Integer.MAX_VALUE;

    private PluginModel dlq;

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

    public Builder withDlq(final PluginModel dlq) {
      checkNotNull(dlq, "dlq cannot be null");
      this.dlq = dlq;
      return this;
    }
    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(final Builder builder) {
    this.dlqFile = builder.dlqFile;
    this.maxRetries = builder.maxRetries;
    this.dlq = builder.dlq;
  }

  public static RetryConfiguration readRetryConfig(final PluginSetting pluginSetting) {
    RetryConfiguration.Builder builder = new RetryConfiguration.Builder();
    final String dlqFile = (String) pluginSetting.getAttributeFromSettings(DLQ_FILE);
    if (dlqFile != null) {
      builder = builder.withDlqFile(dlqFile);
    }
    final Integer maxRetries = pluginSetting.getIntegerOrDefault(MAX_RETRIES, null);
    if (maxRetries != null) {
      builder = builder.withMaxRetries(maxRetries);
    }
    final LinkedHashMap<String, Map<String, Object>> dlq = (LinkedHashMap) pluginSetting.getAttributeFromSettings(DLQ);
    if (dlq != null) {
      if (dlqFile != null) {
        final String dlqOptionConflictMessage = String.format("%s option cannot be used along with %s option", DLQ_FILE, DLQ);
        throw new RuntimeException(dlqOptionConflictMessage);
      }
      if (dlq.size() != 1) {
        throw new RuntimeException("dlq option must declare exactly one dlq configuration");
      }
      final Map.Entry<String, Map<String, Object>> entry = dlq.entrySet().stream()
          .findFirst()
          .get();
      builder = builder.withDlq(new PluginModel(entry.getKey(), entry.getValue()));
    }
    return builder.build();
  }
}
