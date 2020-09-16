package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";
  public static final String RETRY_STATUS = "retry_status";

  private final String dlqFile;
  private final Set<Integer> retryStatus;

  public String getDlqFile() {
    return dlqFile;
  }

  public Set<Integer> getRetryStatus() {
    return retryStatus;
  }

  public static class Builder {
    private String dlqFile;
    private List<Integer> retryStatus = Collections.singletonList(RestStatus.TOO_MANY_REQUESTS.getStatus());

    public Builder withDlqFile(final String dlqFile) {
      checkNotNull(dlqFile, "dlqFile cannot be null.");
      this.dlqFile = dlqFile;
      return this;
    }

    public Builder withRetryStatus(final List<Integer> retryStatus) {
      checkNotNull(retryStatus, "retryStatus cannot be null.");
      this.retryStatus = retryStatus;
      return this;
    }

    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(final Builder builder) {
    this.dlqFile = builder.dlqFile;
    this.retryStatus = new HashSet<>(builder.retryStatus);
  }
}
