package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

import java.util.Collections;
import java.util.Map;

public class AwsOption {
    public static final String REGION = "region";
    public static final String STS_ROLE_ARN = "sts_role_arn";
    public static final String STS_EXTERNAL_ID = "sts_external_id";
    public static final String STS_HEADER_OVERRIDES = "sts_header_overrides";
    public static final String SERVERLESS = "serverless";
    public static final String SERVERLESS_OPTIONS = "serverless_options";

    static final String DEFAULT_AWS_REGION = "us-east-1";

    @JsonProperty(SERVERLESS)
    private boolean serverless;

    @JsonProperty(REGION)
    private String region = DEFAULT_AWS_REGION;

    @JsonProperty(STS_ROLE_ARN)
    @Size(max = 2048, message = "sts_role_arn length cannot exceed 2048.")
    private String stsRoleArn;

    @JsonProperty(STS_EXTERNAL_ID)
    private String stsExternalId;

    @JsonProperty(STS_HEADER_OVERRIDES)
    private Map<String, String> stsHeaderOverrides = Collections.emptyMap();

    @JsonProperty(SERVERLESS_OPTIONS)
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
}
