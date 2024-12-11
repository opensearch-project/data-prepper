package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import lombok.Getter;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ActionConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.DlqConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateType;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;

import java.util.List;
import java.util.Objects;

public class OpenSearchSinkConfig {
    @Getter
    @JsonProperty("hosts")
    private List<String> hosts;

    @Getter
    @JsonProperty("username")
    private String username = null;

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
    private Boolean insecure = false;

    @Getter
    @JsonProperty("proxy")
    private String proxy = null;

    @Getter
    @JsonProperty("distribution_version")
    private String distributionVersion = DistributionVersion.DEFAULT.getVersion();

    @JsonProperty("enable_request_compression")
    private Boolean enableRequestCompression = null;

    public Boolean getEnableRequestCompression(boolean defaultValue) {
        return Objects.requireNonNullElse(enableRequestCompression, defaultValue);
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

    @JsonProperty("bulk_size")
    private Long bulkSize;

    public Long getBulkSize(Long defaultBulkSize) {
        return bulkSize == null ? defaultBulkSize : bulkSize;
    }

    @JsonProperty("estimate_bulk_size_using_compression")
    private Boolean estimateBulkSizeUsingCompression;

    public Boolean getEstimateBulkSizeUsingCompression(boolean defaultValue) {
        return Objects.requireNonNullElse(estimateBulkSizeUsingCompression, defaultValue);
    }

    @JsonProperty("max_local_compressions_for_estimation")
    private Integer maxLocalCompressionsForEstimation;

    public Integer getMaxLocalCompressionsForEstimation(Integer defaultValue) {
        return Objects.requireNonNullElse(maxLocalCompressionsForEstimation, defaultValue);
    }

    @JsonProperty("flush_timeout")
    private Long flushTimeout = null;

    public Long getFlushTimeout(Long defaultFlushTimeout) {
        return flushTimeout == null ? defaultFlushTimeout : flushTimeout;
    }

    @Getter
    @JsonProperty("document_version_type")
    private String versionType = null;

    @Getter
    @JsonProperty("document_version")
    private String versionExpression = null;

    @Getter
    @JsonProperty("normalize_index")
    private Boolean normalizeIndex = false;

    @Getter
    @JsonProperty("document_id")
    private String documentId = null;

    @Getter
    @JsonProperty("routing")
    private String routing = null;

    @Getter
    @JsonProperty("ism_policy_file")
    private String ismPolicyFile = null;

    @Getter
    @JsonProperty("action")
    private String action = OpenSearchBulkActions.INDEX.toString();

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



    public void validateConfig() {
        isActionValid();
        isDlqValid();
    }

    void isDlqValid() {
        if (dlq != null) {
            if (dlqFile!= null) {
                throw new IllegalArgumentException("dlq_file option cannot be used along with dlq option");
            }
        }
    }

    void isActionValid() {
        if (action.equals("index") || action.equals("create") || action.equals("update") || action.equals("upsert") || action.equals("delete")) {
            return;
        }
        throw new IllegalArgumentException("action must be one of [index, create, update, upsert, delete]");
    }

}

