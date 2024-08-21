/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.personalize.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.GroupSequence;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.arns.Arn;

import java.util.Map;
import java.util.Optional;

@GroupSequence({AwsAuthenticationOptions.class, PersonalizeAdvancedValidation.class})
public class AwsAuthenticationOptions {
    private static final String AWS_IAM_ROLE = "role";
    private static final String AWS_IAM = "iam";

    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_external_id")
    @Size(min = 2, max = 1224, message = "awsStsExternalId length should be between 2 and 1224 characters")
    private String awsStsExternalId;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @AssertTrue(message = "sts_role_arn must be an IAM Role", groups = PersonalizeAdvancedValidation.class)
    boolean isValidStsRoleArn() {
        if (awsStsRoleArn == null) {
            return true;
        }
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

    public Optional<Region> getAwsRegion() {
        Region region = awsRegion != null ? Region.of(awsRegion) : null;
        return Optional.ofNullable(region);
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public String getAwsStsExternalId() {
        return awsStsExternalId;
    }

    public Map<String, String> getAwsStsHeaderOverrides() {
        return awsStsHeaderOverrides;
    }
}