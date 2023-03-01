/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3ClientProvider;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3DLQWriter;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RetryConfiguration {
  public static final String DLQ_FILE = "dlq_file";
  public static final String DLQ_S3_BUCKET_NAME = "dlq_s3_bucket";
  public static final String DLQ_S3_KEY_PREFIX = "dlq_s3_key_prefix";
  public static final String S3_AWS_REGION = "s3_aws_region";
  public static final String S3_AWS_STS_ROLE_ARN = "s3_aws_sts_role_arn";
  private static final String DEFAULT_AWS_REGION = "us-east-1";

  private final String dlqFile;
  private final String s3BucketName;
  private final String s3KeyPrefix;
  private final S3Client s3Client;

  public String getDlqFile() {
    return dlqFile;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public String getS3KeyPrefix() {
    return s3KeyPrefix;
  }

  public S3Client getS3Client() {
    return s3Client;
  }

  public static class Builder {
    private String dlqFile;
    private String s3BucketName;
    private String s3KeyPrefix;
    private S3Client s3Client;

    public Builder withDlqFile(final String dlqFile) {
      checkNotNull(dlqFile, "dlqFile cannot be null.");
      this.dlqFile = dlqFile;
      return this;
    }

    public Builder withS3BucketName(final String s3BucketName) {
      checkNotNull(s3BucketName, "s3BucketName cannot be null.");
      this.s3BucketName = s3BucketName;
      return this;
    }

    public Builder withS3KeyPrefix(final String s3KeyPrefix) {
      checkNotNull(s3KeyPrefix, "s3KeyPrefix cannot be null.");
      this.s3KeyPrefix = s3KeyPrefix;
      return this;
    }

    public Builder withS3Client(final S3Client s3Client) {
      checkArgument(s3Client != null);
      this.s3Client = s3Client;
      return this;
    }

    public RetryConfiguration build() {
      return new RetryConfiguration(this);
    }
  }

  private RetryConfiguration(final Builder builder) {
    this.dlqFile = builder.dlqFile;
    this.s3BucketName = builder.s3BucketName;
    this.s3KeyPrefix = builder.s3KeyPrefix;
    this.s3Client = builder.s3Client;
  }

  public static RetryConfiguration readRetryConfig(final PluginSetting pluginSetting) {
    RetryConfiguration.Builder builder = new RetryConfiguration.Builder();
    final String dlqFile = (String) pluginSetting.getAttributeFromSettings(DLQ_FILE);
    final String s3BucketName = (String) pluginSetting.getAttributeFromSettings(DLQ_S3_BUCKET_NAME);
    final String s3KeyPrefix = pluginSetting.getStringOrDefault(DLQ_S3_KEY_PREFIX, "");
    if (dlqFile != null) {
      builder = builder.withDlqFile(dlqFile);
    }
    if (s3BucketName != null) {
      builder = builder.withS3BucketName(s3BucketName);
      builder = builder.withS3KeyPrefix(s3KeyPrefix);
      final String s3AwsRegion = pluginSetting.getStringOrDefault(S3_AWS_REGION, DEFAULT_AWS_REGION);
      final String s3AwsStsRoleArn = pluginSetting.getStringOrDefault(S3_AWS_STS_ROLE_ARN, null);
      final S3ClientProvider clientProvider = new S3ClientProvider(s3AwsRegion, s3AwsStsRoleArn);
      builder.withS3Client(clientProvider.buildS3Client());
    }
    return builder.build();
  }

  public DLQWriter createDLQWriter() throws IOException {
    if (s3BucketName != null) {
      return new S3DLQWriter(s3Client, s3BucketName, s3KeyPrefix);
    } else if (dlqFile != null) {
      return new LocalFileDLQWriter(dlqFile);
    } else {
      return null;
    }
  }
}
