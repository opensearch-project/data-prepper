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
import org.opensearch.dataprepper.aws.api.AwsCredentialsConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kms.KmsClient;

import java.util.Map;
import java.util.Optional;

@JsonTypeName(KmsEncryptionEngineConfiguration.NAME)
public class KmsEncryptionEngineConfiguration implements EncryptionEngineConfiguration, AwsCredentialsConfig {
    private static final String AWS_IAM = "iam";
    private static final String AWS_IAM_ROLE = "role";
    static final String NAME = "kms";

    @JsonProperty("encryption_key")
    private String encryptionKey;

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

    public String getEncryptionKey() {
        return encryptionKey;
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

    public KmsClient createKmsClient() {
        final AwsContext awsContext = new AwsContext(this);
        final AwsCredentialsProvider awsCredentialsProvider = awsContext.getOrDefault();
        return KmsClient.builder()
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

    private Arn getArn() {
        try {
            return Arn.fromString(stsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for sts_role_arn. Check the format of %s", stsRoleArn));
        }
    }
}
