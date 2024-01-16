package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;

import java.util.Map;

public class KmsConfig implements AwsCredentialsConfig {
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

    @JsonProperty("role_session_name")
    private String stsRoleSessionName;

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
}
