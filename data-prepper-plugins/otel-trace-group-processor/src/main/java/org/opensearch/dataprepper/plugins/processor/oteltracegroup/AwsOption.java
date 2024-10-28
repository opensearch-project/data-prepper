package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AwsOption {
    public static final String REGION = "region";
    public static final String STS_ROLE_ARN = "sts_role_arn";
    public static final String STS_EXTERNAL_ID = "sts_external_id";
    public static final String STS_HEADER_OVERRIDES = "sts_header_overrides";
    public static final String SERVERLESS = "serverless";
    public static final String SERVERLESS_OPTIONS = "serverless_options";
    private static final String AWS_IAM = "iam";
    private static final String AWS_IAM_ROLE = "role";

    static final String DEFAULT_AWS_REGION = "us-east-1";

    @JsonProperty(SERVERLESS)
    private boolean serverless;

    @JsonProperty(REGION)
    @JsonPropertyDescription("A string that represents the AWS Region of the Amazon OpenSearch Service domain, " +
            "for example, <code>us-west-2</code>. Only applies to Amazon OpenSearch Service.")
    private String region = DEFAULT_AWS_REGION;

    @JsonProperty(STS_ROLE_ARN)
    @JsonPropertyDescription("An AWS Identity and Access Management (IAM) role that the sink plugin assumes to sign the request to Amazon OpenSearch Service. " +
            "If not provided, the plugin uses the <a href=\"https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html\">default credentials</a>.")
    @Size(max = 2048, message = "sts_role_arn length cannot exceed 2048.")
    private String stsRoleArn;

    @JsonProperty(STS_EXTERNAL_ID)
    @JsonPropertyDescription("The external ID to attach to AssumeRole requests from AWS STS.")
    private String stsExternalId;

    @JsonProperty(STS_HEADER_OVERRIDES)
    @JsonPropertyDescription("A map of header overrides that the IAM role assumes for the plugin.")
    private Map<String, String> stsHeaderOverrides = Collections.emptyMap();

    @JsonProperty(SERVERLESS_OPTIONS)
    @JsonPropertyDescription("The network configuration options available when the backend of the <code>opensearch</code> sink is set to Amazon OpenSearch Serverless.")
    private ServerlessOptions serverlessOptions;

    public String getRegion() {
        return region;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public String getStsExternalId() {
        return stsExternalId;
    }

    public Map<String, String> getStsHeaderOverrides() {
        return stsHeaderOverrides;
    }

    public boolean isServerless() {
        return serverless;
    }

    public ServerlessOptions getServerlessOptions() {
        return serverlessOptions;
    }

    @AssertTrue(message = "sts_role_arn must be an null or a valid IAM role ARN.")
    boolean isValidStsRoleArn() {
        if (stsRoleArn == null) {
            return true;
        }
        final Arn arn = getArn(stsRoleArn);
        if (!AWS_IAM.equals(arn.service())) {
            return false;
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            return false;
        }
        return true;
    }

    private Arn getArn(final String awsStsRoleArn) {
        try {
            return Arn.fromString(awsStsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", awsStsRoleArn));
        }
    }
}
