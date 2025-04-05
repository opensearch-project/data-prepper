/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ActionConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.model.QueryForExistingDocumentConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.FileReader;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3ClientProvider;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_BULK_SIZE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_ESTIMATE_BULK_SIZE_USING_COMPRESSION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_FLUSH_TIMEOUT;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION;

public class IndexConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(IndexConfiguration.class);

    private static final String ACTION_ON_FOUND_DROP = "drop";
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String SETTINGS = "settings";
    public static final String INDEX_ALIAS = "index";
    public static final String INDEX_TYPE = "index_type";
    public static final String TEMPLATE_TYPE = "template_type";
    public static final String TEMPLATE_FILE = "template_file";
    public static final String TEMPLATE_CONTENT = "template_content";
    public static final String NUM_SHARDS = "number_of_shards";
    public static final String NUM_REPLICAS = "number_of_replicas";
    public static final String BULK_SIZE = "bulk_size";
    public static final String ESTIMATE_BULK_SIZE_USING_COMPRESSION = "estimate_bulk_size_using_compression";
    public static final String MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION = "max_local_compressions_for_estimation";
    public static final String FLUSH_TIMEOUT = "flush_timeout";
    public static final String DOCUMENT_ID_FIELD = "document_id_field";
    public static final String DOCUMENT_ID = "document_id";
    public static final String ROUTING_FIELD = "routing_field";
    public static final String ROUTING = "routing";
    public static final String PIPELINE = "pipeline";
    public static final String ISM_POLICY_FILE = "ism_policy_file";
    public static final String ACTION = "action";
    public static final String ACTIONS = "actions";
    public static final String SERVERLESS = "serverless";
    public static final String DISTRIBUTION_VERSION = "distribution_version";
    public static final String AWS_OPTION = "aws";
    public static final String DOCUMENT_ROOT_KEY = "document_root_key";
    public static final String DOCUMENT_VERSION_EXPRESSION = "document_version";

    private IndexType indexType;
    private TemplateType templateType;
    private final String indexAlias;
    private final Map<String, Object> indexTemplate;
    private final String documentIdField;
    private final String documentId;
    private final String routingField;
    private final String routing;
    private final long bulkSize;
    private final boolean estimateBulkSizeUsingCompression;
    private int maxLocalCompressionsForEstimation;
    private final long flushTimeout;
    private final Optional<String> ismPolicyFile;
    private final String action;
    private final List<ActionConfiguration> actions;
    private final String s3AwsRegion;
    private final String s3AwsStsRoleArn;
    private final String s3AwsExternalId;
    private final S3Client s3Client;
    private final boolean serverless;
    private final DistributionVersion distributionVersion;
    private final String documentRootKey;
    private final String versionExpression;
    private final VersionType versionType;
    private final boolean normalizeIndex;

    private final String queryWhen;

    private final Duration queryDuration;

    private final String queryTerm;

    private final String queryActionOnFound;

    private final boolean queryOnBulkFailures;

    private final Integer queryAsyncDocumentLimit;

    private static final String S3_PREFIX = "s3://";

    @SuppressWarnings("unchecked")
    private IndexConfiguration(final Builder builder) {
        this.serverless = builder.serverless;
        this.distributionVersion = builder.distributionVersion;
        determineIndexType(builder);

        this.s3AwsRegion = builder.s3AwsRegion;
        this.s3AwsStsRoleArn = builder.s3AwsStsRoleArn;
        this.s3AwsExternalId = builder.s3AwsStsExternalId;
        this.s3Client = builder.s3Client;
        this.versionExpression = builder.versionExpression;
        this.versionType = builder.versionType;
        this.normalizeIndex = builder.normalizeIndex;
        this.queryOnBulkFailures = builder.queryOnIndexingFailure;

        determineTemplateType(builder);

        this.indexTemplate = builder.templateContent != null ? readTemplateContent(builder.templateContent) : readIndexTemplate(builder.templateFile, indexType, templateType);

        if (builder.numReplicas > 0) {
            indexTemplate.putIfAbsent(SETTINGS, new HashMap<>());
            ((Map<String, Object>) indexTemplate.get(SETTINGS)).putIfAbsent(NUM_REPLICAS, builder.numReplicas);
        }

        if (builder.numShards > 0) {
            indexTemplate.putIfAbsent(SETTINGS, new HashMap<>());
            ((Map<String, Object>) indexTemplate.get(SETTINGS)).putIfAbsent(NUM_SHARDS, builder.numShards);
        }

        String indexAlias = builder.indexAlias;
        if (IndexConstants.TYPE_TO_DEFAULT_ALIAS.containsKey(indexType)) {
            indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(indexType);
        } else {
            if (indexAlias == null) {
                throw new IllegalStateException("Missing required properties:indexAlias");
            }
        }
        this.indexAlias = indexAlias;
        this.bulkSize = builder.bulkSize;
        this.estimateBulkSizeUsingCompression = builder.estimateBulkSizeUsingCompression;
        this.maxLocalCompressionsForEstimation = builder.maxLocalCompressionsForEstimation;
        this.flushTimeout = builder.flushTimeout;
        this.routingField = builder.routingField;
        this.routing = builder.routing;

        String documentIdField = builder.documentIdField;
        String documentId = builder.documentId;
        if (indexType.equals(IndexType.TRACE_ANALYTICS_RAW) || indexType.equals(IndexType.TRACE_ANALYTICS_RAW_STANDARD)) {
            documentId = "${spanId}";
        } else if (indexType.equals(IndexType.TRACE_ANALYTICS_SERVICE_MAP)) {
            documentId = "${hashId}";
        }
        this.documentIdField = documentIdField;
        this.documentId = documentId;
        this.ismPolicyFile = builder.ismPolicyFile;
        this.action = builder.action;
        this.actions = builder.actions;
        this.documentRootKey = builder.documentRootKey;
        this.queryWhen = builder.queryWhen;
        this.queryTerm = builder.queryTerm;
        this.queryActionOnFound = builder.actionOnFound;
        this.queryDuration = builder.queryDuration;
        this.queryAsyncDocumentLimit = builder.queryAsyncLimit;
    }

    private void determineIndexType(Builder builder) {
        if(builder.indexType != null) {
            Optional<IndexType> mappedIndexType = IndexType.getByValue(builder.indexType);
            indexType = mappedIndexType.orElseThrow(
                    () -> new IllegalArgumentException("Value of the parameter, index_type, must be from the list: "
                    + IndexType.getIndexTypeValues()));
        } else if (builder.serverless) {
            this.indexType = IndexType.MANAGEMENT_DISABLED;
        } else {
            this.indexType  = IndexType.CUSTOM;
        }
    }

    private void determineTemplateType(Builder builder) {
        if (builder.serverless) {
            templateType = TemplateType.INDEX_TEMPLATE;
        } else {
            templateType = DistributionVersion.ES6.equals(builder.distributionVersion) ? TemplateType.V1 :
                    (builder.templateType != null ? builder.templateType : TemplateType.V1);
        }
    }

    public static IndexConfiguration readIndexConfig(final OpenSearchSinkConfig openSearchSinkConfig) {
        return readIndexConfig(openSearchSinkConfig, null);
    }

    public static IndexConfiguration readIndexConfig(final OpenSearchSinkConfig openSearchSinkConfig, final ExpressionEvaluator expressionEvaluator) {
        IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
        final String indexAlias = openSearchSinkConfig.getIndexAlias();
        if (indexAlias != null) {
            builder = builder.withIndexAlias(indexAlias);
        }
        final String indexType = openSearchSinkConfig.getIndexType();
        if (indexType != null) {
            builder = builder.withIndexType(indexType);
        }
        final String templateType = openSearchSinkConfig.getTemplateType();
        if (templateType != null) {
            builder = builder.withTemplateType(templateType);
        }
        final String templateFile = openSearchSinkConfig.getTemplateFile();
        if (templateFile != null) {
            builder = builder.withTemplateFile(templateFile);
        }
        final String templateContent = openSearchSinkConfig.getTemplateContent();
        if (templateContent != null) {
            builder = builder.withTemplateContent(templateContent);
        }
        if (templateContent != null && templateFile != null) {
            LOG.warn("Both template_content and template_file are configured. Only template_content will be used");
        }

        final String documentIdField = openSearchSinkConfig.getDocumentIdField();
        final String documentId = openSearchSinkConfig.getDocumentId();
        if (documentIdField != null) {
            LOG.warn("document_id_field is deprecated in favor of document_id, and support for document_id_field will be removed in a future major version release.");
            builder = builder.withDocumentIdField(documentIdField);
        } else if (documentId != null) {
            builder = builder.withDocumentId(documentId);
        }
        final String routingField = openSearchSinkConfig.getRoutingField();
        final String routing = openSearchSinkConfig.getRouting();
        if (routingField != null) {
            LOG.warn("routing_field is deprecated in favor of routing, and support for routing_field will be removed in a future major version release.");
            builder = builder.withRoutingField(routingField);
        } else if (routing != null) {
            builder = builder.withRouting(routing);
        }

        final QueryForExistingDocumentConfiguration queryExistingConfiguration = openSearchSinkConfig.getQueryExistingConfiguration();
        if (queryExistingConfiguration != null) {
            if (!expressionEvaluator.isValidExpressionStatement(queryExistingConfiguration.getQueryWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("query_when %s is not a valid conditional expression statement", openSearchSinkConfig.getQueryExistingConfiguration()));
            }

            builder.withQueryWhen(queryExistingConfiguration.getQueryWhen());
            builder.withQueryTerm(queryExistingConfiguration.getQueryTerm());
            builder.withQueryDuration(queryExistingConfiguration.getQueryDuration());
            builder.withQueryOnIndexingFailure(queryExistingConfiguration.isQueryOnBulkErrors());
            builder.withQueryAsyncLimit(queryExistingConfiguration.getAsyncDocumentLimit());
            builder.withActionOnFound(ACTION_ON_FOUND_DROP);
        }

        builder = builder.withNumShards(openSearchSinkConfig.getNumShards())
                .withNumReplicas(openSearchSinkConfig.getNumReplicas())
                .withBulkSize(openSearchSinkConfig.getBulkSize())
                .withEstimateBulkSizeUsingCompression(openSearchSinkConfig.isEstimateBulkSizeUsingCompression())
                .withMaxLocalCompressionsForEstimation(openSearchSinkConfig.getMaxLocalCompressionsForEstimation())
                .withFlushTimeout(openSearchSinkConfig.getFlushTimeout())
                .withVersionType(openSearchSinkConfig.getVersionType())
                .withNormalizeIndex(openSearchSinkConfig.isNormalizeIndex())
                .withIsmPolicyFile(openSearchSinkConfig.getIsmPolicyFile())
                .withDocumentRootKey(openSearchSinkConfig.getDocumentRootKey())
                .withDistributionVersion(openSearchSinkConfig.getDistributionVersion());


        final String versionExpression = openSearchSinkConfig.getVersionExpression();
        builder = builder.withVersionExpression(versionExpression);
        if (versionExpression != null && (!expressionEvaluator.isValidFormatExpression(versionExpression))) {
            throw new InvalidPluginConfigurationException(
                    String.format("document_version \"%s\" is not a valid format expression.", versionExpression));
        }

        List<ActionConfiguration> actionList = openSearchSinkConfig.getActions();
        if (actionList != null) {
            builder = builder.withActions(actionList, expressionEvaluator);
        } else {
            builder = builder.withAction(openSearchSinkConfig.getAction(), expressionEvaluator);
        }

        AwsAuthenticationConfiguration awsAuthenticationConfiguration = openSearchSinkConfig.getAwsAuthenticationOptions();
        if (awsAuthenticationConfiguration != null) {
            builder = builder.withServerless(awsAuthenticationConfiguration.isServerlessCollection());
        } else {
            builder = builder.withServerless(false);
        }

        if ((builder.templateFile != null && builder.templateFile.startsWith(S3_PREFIX))
                || (builder.ismPolicyFile.isPresent() && builder.ismPolicyFile.get().startsWith(S3_PREFIX))) {
            builder.withS3AwsRegion(awsAuthenticationConfiguration.getAwsRegion().toString());
            builder.withS3AWSStsRoleArn(awsAuthenticationConfiguration.getAwsStsRoleArn());
            builder.withS3AWSStsExternalId(awsAuthenticationConfiguration.getAwsStsExternalId());

            final S3ClientProvider clientProvider = new S3ClientProvider(
                    builder.s3AwsRegion, builder.s3AwsStsRoleArn, builder.s3AwsStsExternalId);
            builder.withS3Client(clientProvider.buildS3Client());
        }
        return builder.build();
    }


    public IndexType getIndexType() {
        return indexType;
    }

    public TemplateType getTemplateType() {
        return templateType;
    }

    public String getIndexAlias() {
        return indexAlias;
    }

    public Map<String, Object> getIndexTemplate() {
        return indexTemplate;
    }

    public String getDocumentIdField() {
        return documentIdField;
    }

    public String getDocumentId() { return documentId; }

    public String getRoutingField() {
        return routingField;
    }

    public String getRouting() {
        return routing;
    }

    public long getBulkSize() {
        return bulkSize;
    }

    public boolean isEstimateBulkSizeUsingCompression() {
        return estimateBulkSizeUsingCompression;
    }

    public int getMaxLocalCompressionsForEstimation() {
        return maxLocalCompressionsForEstimation;
    }

    public long getFlushTimeout() {
        return flushTimeout;
    }

    public Optional<String> getIsmPolicyFile() {
        return ismPolicyFile;
    }

    public String getAction() {
        return action;
    }

    public List<ActionConfiguration> getActions() {
        return actions;
    }

    public String getS3AwsRegion() {
        return s3AwsRegion;
    }

    public String getS3AwsStsRoleArn() {
        return s3AwsStsRoleArn;
    }

    public String getS3AwsStsExternalId() {
        return s3AwsExternalId;
    }

    public boolean getServerless() {
        return serverless;
    }

    public DistributionVersion getDistributionVersion() {
        return distributionVersion;
    }

    public String getDocumentRootKey() {
        return documentRootKey;
    }

    public VersionType getVersionType() { return versionType; }

    public String getVersionExpression() { return versionExpression; }

    public boolean isNormalizeIndex() { return normalizeIndex; }

    public String getQueryWhen() { return queryWhen; }

    public Duration getQueryDuration() { return queryDuration; }

    public String getQueryTerm() { return queryTerm; }

    public boolean getQueryOnBulkFailures() {
        return queryOnBulkFailures;
    }

    public Integer getQueryAsyncDocumentLimit() {return queryAsyncDocumentLimit; }

    /**
     * This method is used in the creation of IndexConfiguration object. It takes in the template file path
     * or index type and returns the index template read from the file or specific to index type or returns an
     * empty map.
     *
     * @param templateFile
     * @param indexType
     * @param templateType
     * @return
     */
    private Map<String, Object> readIndexTemplate(final String templateFile, final IndexType indexType, TemplateType templateType) {
        try {
            URL templateURL = null;
            InputStream s3TemplateFile = null;
            if (indexType.equals(IndexType.TRACE_ANALYTICS_RAW)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.RAW_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.TRACE_ANALYTICS_RAW_STANDARD)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.RAW_STANDARD_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.TRACE_ANALYTICS_SERVICE_MAP)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.LOG_ANALYTICS)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.LOGS_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.LOG_ANALYTICS_STANDARD)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.LOGS_STANDARD_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.METRIC_ANALYTICS)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.METRICS_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.METRIC_ANALYTICS_STANDARD)) {
                templateURL = loadExistingTemplate(templateType, IndexConstants.METRICS_STANDARD_TEMPLATE_FILE);
            } else if (templateFile != null) {
                if (templateFile.toLowerCase().startsWith(S3_PREFIX)) {
                    FileReader s3FileReader = new S3FileReader(s3Client);
                    s3TemplateFile = s3FileReader.readFile(templateFile);
                } else {
                    templateURL = new File(templateFile).toURI().toURL();
                }
            }

            if (templateURL != null) {
                return new ObjectMapper().readValue(templateURL, new TypeReference<Map<String, Object>>() {
                });
            } else if (s3TemplateFile != null) {
                return new ObjectMapper().readValue(s3TemplateFile, new TypeReference<Map<String, Object>>() {
                });
            } else {
                return new HashMap<>();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Index template is not valid.", ex);
        }
    }

    private Map<String, Object> readTemplateContent(final String templateContent) {
        try {
            return OBJECT_MAPPER.readValue(templateContent, new TypeReference<Map<String, Object>>() {});
        } catch (IOException ex) {
            throw new InvalidPluginConfigurationException(String.format("template_content is invalid: %s", ex.getMessage()));
        }
    }

    private URL loadExistingTemplate(TemplateType templateType, String predefinedTemplateName) {
        String resourcePath = templateType == TemplateType.V1 ? predefinedTemplateName : templateType.getTypeName() + "/" + predefinedTemplateName;
        return getClass().getClassLoader()
                .getResource(resourcePath);
    }

    public static class Builder {
        private String indexAlias;
        private String indexType;
        private TemplateType templateType;
        private String templateFile;
        private String templateContent;
        private int numShards;
        private int numReplicas;
        private String routingField;
        private String routing;
        private String pipeline;
        private String documentIdField;
        private String documentId;
        private long bulkSize = DEFAULT_BULK_SIZE;
        private boolean estimateBulkSizeUsingCompression = DEFAULT_ESTIMATE_BULK_SIZE_USING_COMPRESSION;
        private int maxLocalCompressionsForEstimation = DEFAULT_MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION;
        private long flushTimeout = DEFAULT_FLUSH_TIMEOUT;
        private Optional<String> ismPolicyFile;
        private String action;
        private List<ActionConfiguration> actions;
        private String s3AwsRegion;
        private String s3AwsStsRoleArn;
        private String s3AwsStsExternalId;
        private S3Client s3Client;
        private boolean serverless;
        private DistributionVersion distributionVersion;
        private String documentRootKey;
        private VersionType versionType;
        private String versionExpression;
        private boolean normalizeIndex;

        private String queryTerm;
        private String queryWhen;
        private String actionOnFound;
        private Duration queryDuration;
        private boolean queryOnIndexingFailure;

        private Integer queryAsyncLimit;

        public Builder withIndexAlias(final String indexAlias) {
            checkArgument(indexAlias != null, "indexAlias cannot be null.");
            checkArgument(!indexAlias.isEmpty(), "indexAlias cannot be empty");
            this.indexAlias = indexAlias;
            return this;
        }

        public Builder withIndexType(final String indexType) {
            checkArgument(indexType != null, "indexType cannot be null.");
            checkArgument(!indexType.isEmpty(), "indexType cannot be empty");
            this.indexType = indexType;
            return this;
        }

        public Builder withTemplateType(final String templateType) {
            checkArgument(templateType != null, "templateType cannot be null.");
            checkArgument(!templateType.isEmpty(), "templateType cannot be empty");
            this.templateType = TemplateType.fromTypeName(templateType);
            return this;
        }

        public Builder withTemplateFile(final String templateFile) {
            checkArgument(templateFile != null, "templateFile cannot be null.");
            this.templateFile = templateFile;
            return this;
        }

        public Builder withTemplateContent(final String templateContent) {
            checkArgument(templateContent != null, "templateContent cannot be null.");
            this.templateContent = templateContent;
            return this;
        }

        public Builder withDocumentIdField(final String documentIdField) {
            checkNotNull(documentIdField, "document_id_field cannot be null");
            this.documentIdField = documentIdField;
            return this;
        }

        public Builder withDocumentId(final String documentId) {
            checkNotNull(documentId, "document_id cannot be null");
            this.documentId = documentId;
            return this;
        }

        public Builder withRoutingField(final String routingField) {
            this.routingField = routingField;
            return this;
        }

        public Builder withRouting(final String routing) {
            this.routing = routing;
            return this;
        }

        public Builder withBulkSize(final long bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        public Builder withEstimateBulkSizeUsingCompression(final boolean estimateBulkSizeUsingCompression) {
            this.estimateBulkSizeUsingCompression = estimateBulkSizeUsingCompression;
            return this;
        }

        public Builder withMaxLocalCompressionsForEstimation(final int maxLocalCompressionsForEstimation) {
            this.maxLocalCompressionsForEstimation = maxLocalCompressionsForEstimation;
            return this;
        }

        public Builder withFlushTimeout(final long flushTimeout) {
            this.flushTimeout = flushTimeout;
            return this;
        }

        public Builder withNumShards(final int numShards) {
            this.numShards = numShards;
            return this;
        }

        public Builder withNumReplicas(final int numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        public Builder withIsmPolicyFile(final String ismPolicyFile) {
            this.ismPolicyFile = Optional.ofNullable(ismPolicyFile);
            return this;
        }

        public Builder withAction(final String action, final ExpressionEvaluator expressionEvaluator) {
            checkArgument((EnumUtils.isValidEnumIgnoreCase(OpenSearchBulkActions.class, action) ||
                    (action.contains("${") && expressionEvaluator.isValidFormatExpression(action))), "action \"" + action + "\" is invalid. action must be one of the following: " + Arrays.stream(OpenSearchBulkActions.values()).collect(Collectors.toList()));
            this.action = action;
            return this;
        }

        public Builder withActions(final List<ActionConfiguration> actions, final ExpressionEvaluator expressionEvaluator) {
            for (final ActionConfiguration actionConfig: actions) {
                String action = actionConfig.getType();
                if (action != null) {
                    checkArgument((EnumUtils.isValidEnumIgnoreCase(OpenSearchBulkActions.class, action) ||
                            (action.contains("${") && expressionEvaluator.isValidFormatExpression(action))), "action \"" + action + "\". action must be one of the following: " + Arrays.stream(OpenSearchBulkActions.values()).collect(Collectors.toList()));
                }
            }
            this.actions = actions;
            return this;
        }

        public Builder withS3AwsRegion(final String s3AwsRegion) {
            checkNotNull(s3AwsRegion, "s3AwsRegion cannot be null");
            this.s3AwsRegion = s3AwsRegion;
            return this;
        }

        public Builder withS3AWSStsRoleArn(final String s3AwsStsRoleArn) {
            checkArgument(s3AwsStsRoleArn == null || s3AwsStsRoleArn.length() <= 2048, "s3AwsStsRoleArn length cannot exceed 2048");
            if(s3AwsStsRoleArn != null) {
                try {
                    Arn.fromString(s3AwsStsRoleArn);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid ARN format for s3AwsStsRoleArn");
                }
            }
            this.s3AwsStsRoleArn = s3AwsStsRoleArn;
            return this;
        }

        public Builder withS3AWSStsExternalId(final String s3AwsStsExternalId) {
            checkArgument(s3AwsStsExternalId == null || s3AwsStsExternalId.length() <= 1224, "s3AwsStsExternalId length cannot exceed 1224");
            this.s3AwsStsExternalId = s3AwsStsExternalId;
            return this;
        }

        public Builder withS3Client(final S3Client s3Client) {
            checkArgument(s3Client != null);
            this.s3Client = s3Client;
            return this;
        }

        public Builder withServerless(final boolean serverless) {
            this.serverless = serverless;
            return this;
        }

        public Builder withDistributionVersion(final String distributionVersion) {
            this.distributionVersion = DistributionVersion.fromTypeName(distributionVersion);
            return this;
        }

        public Builder withDocumentRootKey(final String documentRootKey) {
            if (documentRootKey != null) {
                checkArgument(!documentRootKey.isEmpty(), "documentRootKey cannot be empty string");
            }
            this.documentRootKey = documentRootKey;
            return this;
        }

        public Builder withVersionType(final String versionType) {
            if (versionType != null) {
                try {
                    this.versionType = getVersionType(versionType);
                } catch (final IllegalArgumentException e) {
                    throw new InvalidPluginConfigurationException(
                            String.format("version_type %s is invalid. version_type must be one of: %s",
                                    versionType, Arrays.stream(VersionType.values()).collect(Collectors.toList())));
                }
            }

            return this;
        }

        public Builder withNormalizeIndex(final boolean normalizeIndex) {
            this.normalizeIndex = normalizeIndex;
            return this;
        }

        private VersionType getVersionType(final String versionType) {
            switch (versionType.toLowerCase()) {
                case "internal":
                    return VersionType.Internal;
                case "external":
                    return VersionType.External;
                case "external_gte":
                    return VersionType.ExternalGte;
                default:
                    throw new IllegalArgumentException();
            }
        }

        public Builder withVersionExpression(final String versionExpression) {
            if (versionExpression != null && !versionExpression.contains("${")) {
                throw new InvalidPluginConfigurationException(
                        String.format("document_version %s is invalid. It must be in the format of \"${/key}\" or \"${expression}\"", versionExpression));
            }

            this.versionExpression = versionExpression;

            return this;
        }

        public Builder withQueryTerm(final String queryTerm) {
            this.queryTerm = queryTerm;
            return this;
        }

        public Builder withQueryWhen(final String queryWhen) {
            this.queryWhen = queryWhen;
            return this;
        }

        public Builder withQueryDuration(final Duration queryDuration) {
            this.queryDuration = queryDuration;
            return this;
        }

        public Builder withActionOnFound(final String actionOnFound) {
            if (!Objects.equals(actionOnFound, ACTION_ON_FOUND_DROP)) {
                throw new InvalidPluginConfigurationException("Only \"drop\" is supported for query_for_existing_document.action_on_found");
            }
            this.actionOnFound = actionOnFound;
            return this;
        }

        public Builder withQueryOnIndexingFailure(final boolean queryOnIndexingFailure) {
            this.queryOnIndexingFailure = queryOnIndexingFailure;
            return this;
        }

        public Builder withQueryAsyncLimit(final Integer queryAsyncLimit) {
            this.queryAsyncLimit = queryAsyncLimit;
            return this;
        }

        public IndexConfiguration build() {
            return new IndexConfiguration(this);
        }
    }
}
