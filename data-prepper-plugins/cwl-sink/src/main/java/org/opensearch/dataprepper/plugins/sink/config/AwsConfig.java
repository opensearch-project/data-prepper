package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.regions.Region;

/**
 * AwsConfig is based on the S3-Sink AwsAuthenticationOptions
 * where the configuration allows the sink to fetch Aws credentials
 * and resources.
 */
public class AwsConfig {
    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("path_to_credentials")
    private String pathToCredentials;

    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public String getPathToCredentials() {
        return pathToCredentials;
    }
}
