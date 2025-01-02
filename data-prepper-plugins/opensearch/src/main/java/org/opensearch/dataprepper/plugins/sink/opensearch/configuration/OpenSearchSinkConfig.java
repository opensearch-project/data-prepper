package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateType;

import java.util.List;
import java.util.Objects;

import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DEFAULT_BULK_SIZE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DEFAULT_ESTIMATE_BULK_SIZE_USING_COMPRESSION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DEFAULT_FLUSH_TIMEOUT;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DEFAULT_MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION;

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

    @AssertTrue(message = "dlq_file option cannot be used along with dlq option")
    public boolean isDlqValid() {
        if (dlq != null) {
            if (dlqFile!= null) {
                return false;
            }
        }
        return true;
    }

    @AssertTrue(message = "action must be one of [index, create, update, upsert, delete]")
    public boolean isActionValid() {
        if (EnumUtils.isValidEnumIgnoreCase(OpenSearchBulkActions.class, action)) {
            return true;
        }
        return false;
    }

}

