/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.DlqConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";
  public static final String MAX_RETRIES = "max_retries";
  public static final String DLQ = "dlq";

  private final String dlqFile;
  private final int maxRetries;
  private final DlqConfiguration dlq;

  public String getDlqFile() {
    return dlqFile;
  }

  public Optional<DlqConfiguration> getDlq() {
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

    private DlqConfiguration dlq;

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

    public Builder withDlq(final DlqConfiguration dlq) {
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

 public static RetryConfiguration readRetryConfig(final OpenSearchSinkConfig openSearchSinkConfig) {
   RetryConfiguration.Builder builder = new RetryConfiguration.Builder();
   final String dlqFile = openSearchSinkConfig.getDlqFile();
   if (dlqFile != null) {
     builder = builder.withDlqFile(dlqFile);
   }
   final Integer maxRetries = openSearchSinkConfig.getMaxRetries();
   if (maxRetries != null) {
     builder = builder.withMaxRetries(maxRetries);
   }
   final DlqConfiguration dlqConfiguration = openSearchSinkConfig.getDlq();
   if (dlqConfiguration != null) {
     builder = builder.withDlq(dlqConfiguration);
   }
   return builder.build();
  }
}
