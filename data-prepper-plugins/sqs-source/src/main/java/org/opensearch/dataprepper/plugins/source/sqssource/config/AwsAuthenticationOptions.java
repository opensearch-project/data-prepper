/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.Optional;


public class AwsAuthenticationOptions {

    private static final String AWS_IAM_ROLE = "role";

    private static final String AWS_IAM = "iam";

    @JsonProperty("sts_region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;



    @AssertTrue(message = "sts_role_arn must be an IAM Role")
    boolean isValidStsRoleArn() {
        final Arn arn = getArn();
        boolean status = true;
        if (!AWS_IAM.equals(arn.service())) {
            status = false;
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            status = false;
        }
        return status;
    }

    private Arn getArn() {
        try {
            return Arn.fromString(awsStsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", awsStsRoleArn));
        }
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }

    public Map<String, String> getAwsStsHeaderOverrides() {
        return awsStsHeaderOverrides;
    }
}