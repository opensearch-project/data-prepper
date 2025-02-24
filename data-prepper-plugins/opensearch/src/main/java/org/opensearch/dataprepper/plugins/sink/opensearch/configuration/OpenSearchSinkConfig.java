/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OpenSearchSinkConfig {
    public static final long DEFAULT_BULK_SIZE = 5L;
    public static final boolean DEFAULT_ESTIMATE_BULK_SIZE_USING_COMPRESSION = false;
    public static final int DEFAULT_MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION = 2;
    public static final long DEFAULT_FLUSH_TIMEOUT = 60_000L;
    public static final String DEFAULT_AWS_REGION = "us-east-1";
    @Getter
    @JsonProperty("hosts")
    private List<String> hosts;

    @Getter
    @JsonProperty("username")
    private String username = null;

    @Getter
    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @AssertTrue(message = "username and password should not be set when authentication is configured.")
    public boolean isAuthConfigValid() {
        return authConfig == null || (username == null && password == null);
    }

    @Getter
    @JsonProperty("password")
    private String password = null;

    @Getter
    @JsonProperty("socket_timeout")
    private Integer socketTimeout = null;

    @Getter
    @JsonProperty("connect_timeout")
    private Integer connectTimeout = null;

    @Getter
    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationConfiguration awsAuthenticationOptions;

    @Getter
    @JsonProperty("cert")
    private String certPath = null;

    @Getter
    @JsonProperty("insecure")
    private boolean insecure = false;

    @Getter
    @JsonProperty("proxy")
    private String proxy = null;

    @Getter
    @JsonProperty("distribution_version")
    private String distributionVersion = DistributionVersion.DEFAULT.getVersion();

    @JsonProperty("enable_request_compression")
    private Boolean enableRequestCompression = null;

    public boolean getEnableRequestCompression() {
        final DistributionVersion distributionVersion = DistributionVersion.fromTypeName(getDistributionVersion());
        return Objects.requireNonNullElse(enableRequestCompression, !DistributionVersion.ES6.equals(distributionVersion));
    }

    @Getter
    @JsonProperty("index")
    private String indexAlias = null;

    @Getter
    @JsonProperty("index_type")
    private String indexType = null;

    @Getter
    @JsonProperty("template_type")
    private String templateType = TemplateType.V1.getTypeName();

    @Getter
    @JsonProperty("template_file")
    private String templateFile = null;

    @Getter
    @JsonProperty("template_content")
    private String templateContent = null;

    @Getter
    @JsonProperty("number_of_shards")
    private Integer numShards = 0;

    @Getter
    @JsonProperty("number_of_replicas")
    private Integer numReplicas = 0;

    @Getter
    @JsonProperty("bulk_size")
    private Long bulkSize = DEFAULT_BULK_SIZE;

    @Getter
    @JsonProperty("estimate_bulk_size_using_compression")
    private boolean estimateBulkSizeUsingCompression = DEFAULT_ESTIMATE_BULK_SIZE_USING_COMPRESSION;

    @Getter
    @JsonProperty("max_local_compressions_for_estimation")
    private Integer maxLocalCompressionsForEstimation = DEFAULT_MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION;

    @Getter
    @JsonProperty("flush_timeout")
    private Long flushTimeout = DEFAULT_FLUSH_TIMEOUT;

    @Getter
    @JsonProperty("document_version_type")
    private String versionType = null;

    @Getter
    @JsonProperty("document_version")
    private String versionExpression = null;

    @Getter
    @JsonProperty("normalize_index")
    private boolean normalizeIndex = false;

    @Getter
    @JsonProperty("document_id")
    private String documentId = null;

    @Getter
    @JsonProperty("routing")
    private String routing = null;

    @Getter
    @JsonProperty("ism_policy_file")
    private String ismPolicyFile = null;

    @JsonProperty("action")
    private OpenSearchBulkActions action = OpenSearchBulkActions.INDEX;

    @AssertTrue(message = "action must be one of index, create, update, upsert, delete")
    boolean isActionValid() {
        if (action == null) {         //action will be null if the string doesn't match one of the enums
            return false;
        }
        return true;
    }

    public String getAction() {
        return action.toString();
    }

    @Getter
    @Valid
    @JsonProperty("actions")
    private List<ActionConfiguration> actions = null;

    @Getter
    @JsonProperty("document_root_key")
    private String documentRootKey = null;

    @Getter
    @JsonProperty("dlq_file")
    private String dlqFile = null;

    @Getter
    @JsonProperty("max_retries")
    private Integer maxRetries = null;

    @Getter
    @JsonProperty("dlq")
    private DlqConfiguration dlq;

    @AssertTrue(message = "dlq_file option cannot be used along with dlq option")
    public boolean isDlqValid() {
        if (dlq != null) {
            if (dlqFile!= null) {
                return false;
            }
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("aws_sigv4")
    private boolean awsSigv4 = false;

    @Deprecated
    @AssertTrue(message = "aws_sigv4 option cannot be used along with aws option. It is preferred to only use aws option as aws_sigv4 is deprecated.")
    public boolean isNotAwsSigv4AndAwsOption() {
        if (awsAuthenticationOptions != null && awsSigv4) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("aws_region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion = DEFAULT_AWS_REGION;

    @Deprecated
    @Getter
    @JsonProperty("aws_sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn = null;

    @Deprecated
    @AssertTrue(message = "aws_sts_role_arn cannot be used along with aws option. It is preferred to only use aws option as aws_sts_role_arn is deprecated.")
    public boolean isNotAwsStsRoleArnAndAws() {
        if (awsAuthenticationOptions != null && awsStsRoleArn != null) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("aws_sts_external_id")
    @Size(min = 2, max = 1224, message = "awsStsExternalId length should be between 2 and 1224 characters")
    private String awsStsExternalId = null;

    @Deprecated
    @AssertTrue(message = "aws_sts_external_id cannot be used along with aws option. It is preferred to only use aws option as aws_sts_external_id is deprecated.")
    public boolean isNotAwsStsExternalIdAndAws() {
        if (awsAuthenticationOptions != null && awsStsExternalId != null) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("aws_sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @Deprecated
    @AssertTrue(message = "aws_sts_header_overrides cannot be used along with aws option. It is preferred to only use aws option as aws_sts_header_overrides is deprecated.")
    public boolean isNotAwsStsHeaderOverridesAndAws() {
        if (awsAuthenticationOptions != null && awsStsHeaderOverrides != null) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("serverless")
    private boolean serverless = false;

    @Deprecated
    @AssertTrue(message = "serverless cannot be used along with aws option. It is preferred to only use aws option as serverless is deprecated.")
    public boolean isNotServerlessAndAws() {
        if (awsAuthenticationOptions != null && serverless) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("serverless_options")
    private ServerlessOptions serverlessOptions = null;

    @Deprecated
    @AssertTrue(message = "serverless_options cannot be used along with aws option.  It is preferred to only use aws option as serverless_options is deprecated.")
    public boolean isNotServerlessOptionsAndAws() {
        if (awsAuthenticationOptions != null && serverlessOptions != null) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("document_id_field")
    private String documentIdField = null;

    @Deprecated
    @AssertTrue(message = "Both document_id_field and document_id cannot be used at the same time. It is preferred to only use document_id as document_id_field is deprecated.")
    public boolean isNotDocumentIdFieldAndDocumentId() {
        if (documentId != null && documentIdField != null) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Getter
    @JsonProperty("routing_field")
    private String routingField = null;
}

