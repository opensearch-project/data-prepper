package com.amazon.dataprepper.plugins.sink.elasticsearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";

  private final String dlqFile;

  public String getDlqFile() {
    return dlqFile;
  }

  public static class Builder {
    private String dlqFile;

    public Builder withDlqFile(final String dlqFile) {
      checkNotNull(dlqFile, "dlqFile cannot be null.");
      this.dlqFile = dlqFile;
      return this;
    }

    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(final Builder builder) {
    this.dlqFile = builder.dlqFile;
  }

  public static RetryConfiguration readRetryConfig(final PluginSetting pluginSetting) {
    RetryConfiguration.Builder builder = new RetryConfiguration.Builder();
    final String dlqFile = (String) pluginSetting.getAttributeFromSettings(DLQ_FILE);
    if (dlqFile != null) {
      builder = builder.withDlqFile(dlqFile);
    }
    return builder.build();
  }
}
