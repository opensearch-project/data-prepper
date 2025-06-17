/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.aws.api.AwsContextImpl;
import org.opensearch.dataprepper.aws.api.AwsContext;
import org.opensearch.dataprepper.aws.api.AwsCredentialsConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@JsonTypeName(KmsEncryptionEngineConfiguration.NAME)
class KmsEncryptionEngineConfiguration implements EncryptionEngineConfiguration, AwsCredentialsConfig {
    static final String S3_PREFIX = "s3://";
    private static final String AWS_IAM = "iam";
    private static final String AWS_IAM_ROLE = "role";
    static final String NAME = "kms";

    @JsonProperty("encryption_key")
    private String encryptionKey;

    @JsonProperty("encryption_key_directory")
    private String encryptionKeyDirectory;

    @JsonProperty("key_id")
    private String keyId;

    @JsonProperty("encryption_context")
    private Map<String, String> encryptionContext;

    @Valid
    @Size(min = 1, message = "Region cannot be empty string")
    @JsonProperty("region")
    private String region;

    @Valid
    @Size(min = 20, max = 2048, message = "sts_role_arn length should be between 20 and 2048 characters")
    @JsonProperty("sts_role_arn")
    private String stsRoleArn;

    @Valid
    @JsonProperty("rotation_interval")
    @DurationMin(hours = 2L, message = "Rotation interval must be at least 2 hours.")
    private Duration rotationInterval = Duration.ofDays(30);

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public String getEncryptionKeyDirectory() {
        return encryptionKeyDirectory;
    }

    public String getKeyId() {
        return keyId;
    }

    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getStsRoleArn() {
        return stsRoleArn;
    }

    @Override
    public AwsCredentialsOptions toCredentialsOptions() {
        return AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(stsRoleArn)
                .build();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean rotationEnabled() {
        return isEncryptionKeyInS3();
    }

    @Override
    public Duration getRotationInterval() {
        return rotationInterval;
    }

    public boolean isEncryptionKeyInS3() {
        return encryptionKeyDirectory != null && encryptionKeyDirectory.startsWith(S3_PREFIX);
    }

    public KmsClient createKmsClient() {
        final AwsContext awsContext = new AwsContextImpl(this);
        final AwsCredentialsProvider awsCredentialsProvider = awsContext.getOrDefault();
        return KmsClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(awsContext.getRegionOrDefault())
                .build();
    }

    public S3Client createS3Client() {
        final AwsContext awsContext = new AwsContextImpl(this);
        final AwsCredentialsProvider awsCredentialsProvider = awsContext.getOrDefault();
        return S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(awsContext.getRegionOrDefault())
                .build();
    }

    @AssertTrue(message = "sts_role_arn must be an IAM Role")
    boolean validateStsRoleArn() {
        final Arn arn = getArn();
        if (!AWS_IAM.equals(arn.service())) {
            return false;
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            return false;
        }
        return true;
    }

    @AssertTrue(message = "Only one of encryption_key or encryption_key_directory must be specified.")
    boolean validateEncryptionKeyAndDirectory() {
        if (encryptionKey == null && encryptionKeyDirectory == null) {
            return false;
        }
        if (encryptionKey != null && encryptionKeyDirectory != null) {
            return false;
        }
        return true;
    }

    private Arn getArn() {
        try {
            return Arn.fromString(stsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for sts_role_arn. Check the format of %s", stsRoleArn));
        }
    }
}
