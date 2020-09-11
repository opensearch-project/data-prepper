package com.amazon.situp.plugins.sink.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";

  private final String dlqFile;

  public String getDlqFile() {
    return dlqFile;
  }

  public static class Builder {

    private String dlqFile;

    public Builder withDlqFile(final String dlqFile) {
      checkArgument(dlqFile != null, "dlqFile cannot be null.");
      this.dlqFile = dlqFile;
      return this;
    }

    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(Builder builder) {
    this.dlqFile = builder.dlqFile;
  }
}
