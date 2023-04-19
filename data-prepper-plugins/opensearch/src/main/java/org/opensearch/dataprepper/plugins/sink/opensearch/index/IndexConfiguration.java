/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkAction;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.FileReader;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3ClientProvider;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3FileReader;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IndexConfiguration {
    public static final String SETTINGS = "settings";
    public static final String INDEX_ALIAS = "index";
    public static final String INDEX_TYPE = "index_type";
    public static final String TEMPLATE_FILE = "template_file";
    public static final String NUM_SHARDS = "number_of_shards";
    public static final String NUM_REPLICAS = "number_of_replicas";
    public static final String BULK_SIZE = "bulk_size";
    public static final String DOCUMENT_ID_FIELD = "document_id_field";
    public static final String ROUTING_FIELD = "routing_field";
    public static final String ISM_POLICY_FILE = "ism_policy_file";
    public static final long DEFAULT_BULK_SIZE = 5L;
    public static final String ACTION = "action";
    public static final String S3_AWS_REGION = "s3_aws_region";
    public static final String S3_AWS_STS_ROLE_ARN = "s3_aws_sts_role_arn";
    public static final String AWS_SERVERLESS = "aws_serverless";
    public static final String AWS_OPTION = "aws";
    public static final String DOCUMENT_ROOT_KEY = "document_root_key";

    private IndexType indexType;
    private final String indexAlias;
    private final Map<String, Object> indexTemplate;
    private final String documentIdField;
    private final String routingField;
    private final long bulkSize;
    private final Optional<String> ismPolicyFile;
    private final String action;
    private final String s3AwsRegion;
    private final String s3AwsStsRoleArn;
    private final S3Client s3Client;
    private final boolean awsServerless;
    private final String documentRootKey;

    private static final String S3_PREFIX = "s3://";
    private static final String DEFAULT_AWS_REGION = "us-east-1";

    @SuppressWarnings("unchecked")
    private IndexConfiguration(final Builder builder) {
        this.awsServerless = builder.awsServerless;
        determineIndexType(builder);

        this.s3AwsRegion = builder.s3AwsRegion;
        this.s3AwsStsRoleArn = builder.s3AwsStsRoleArn;
        this.s3Client = builder.s3Client;

        this.indexTemplate = readIndexTemplate(builder.templateFile, indexType);

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
        this.routingField = builder.routingField;

        String documentIdField = builder.documentIdField;
        if (indexType.equals(IndexType.TRACE_ANALYTICS_RAW)) {
            documentIdField = "spanId";
        } else if (indexType.equals(IndexType.TRACE_ANALYTICS_SERVICE_MAP)) {
            documentIdField = "hashId";
        }
        this.documentIdField = documentIdField;
        this.ismPolicyFile = builder.ismPolicyFile;
        this.action = builder.action;
        this.documentRootKey = builder.documentRootKey;
    }

    private void determineIndexType(Builder builder) {
        if(builder.indexType != null) {
            Optional<IndexType> mappedIndexType = IndexType.getByValue(builder.indexType);
            indexType = mappedIndexType.orElseThrow(
                    () -> new IllegalArgumentException("Value of the parameter, index_type, must be from the list: "
                    + IndexType.getIndexTypeValues()));
        } else if (builder.awsServerless) {
            this.indexType = IndexType.MANAGEMENT_DISABLED;
        } else {
            this.indexType  = IndexType.CUSTOM;
        }
    }

    public static IndexConfiguration readIndexConfig(final PluginSetting pluginSetting) {
        IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
        final String indexAlias = pluginSetting.getStringOrDefault(INDEX_ALIAS, null);
        if (indexAlias != null) {
            builder = builder.withIndexAlias(indexAlias);
        }
        final String indexType = pluginSetting.getStringOrDefault(INDEX_TYPE, null);
        if(indexType != null) {
            builder = builder.withIndexType(indexType);
        }
        final String templateFile = pluginSetting.getStringOrDefault(TEMPLATE_FILE, null);
        if (templateFile != null) {
            builder = builder.withTemplateFile(templateFile);
        }
        builder = builder.withNumShards(pluginSetting.getIntegerOrDefault(NUM_SHARDS, 0));
        builder = builder.withNumReplicas(pluginSetting.getIntegerOrDefault(NUM_REPLICAS, 0));
        final Long batchSize = pluginSetting.getLongOrDefault(BULK_SIZE, DEFAULT_BULK_SIZE);
        builder = builder.withBulkSize(batchSize);
        final String documentId = pluginSetting.getStringOrDefault(DOCUMENT_ID_FIELD, null);
        if (documentId != null) {
            builder = builder.withDocumentIdField(documentId);
        }
        final String routingField = pluginSetting.getStringOrDefault(ROUTING_FIELD, null);
        if (routingField != null) {
            builder = builder.withRoutingField(routingField);
        }

        final String ismPolicyFile = pluginSetting.getStringOrDefault(ISM_POLICY_FILE, null);
        builder = builder.withIsmPolicyFile(ismPolicyFile);

        builder.withAction(pluginSetting.getStringOrDefault(ACTION, BulkAction.INDEX.toString()));

        if ((builder.templateFile != null && builder.templateFile.startsWith(S3_PREFIX))
            || (builder.ismPolicyFile.isPresent() && builder.ismPolicyFile.get().startsWith(S3_PREFIX))) {
            builder.withS3AwsRegion(pluginSetting.getStringOrDefault(S3_AWS_REGION, DEFAULT_AWS_REGION));
            builder.withS3AWSStsRoleArn(pluginSetting.getStringOrDefault(S3_AWS_STS_ROLE_ARN, null));

            final S3ClientProvider clientProvider = new S3ClientProvider(builder.s3AwsRegion, builder.s3AwsStsRoleArn);
            builder.withS3Client(clientProvider.buildS3Client());
        }

        Map<String, Object> awsOption = pluginSetting.getTypedMap(AWS_OPTION, String.class, Object.class);
        boolean awsOptionUsed = false;
        if (awsOption != null && !awsOption.isEmpty()) {
            awsOptionUsed = true;
            builder.withAwsServerless((Boolean)awsOption.getOrDefault(AWS_SERVERLESS.substring(4), false));
        }
        final boolean awsServerless = pluginSetting.getBooleanOrDefault(AWS_SERVERLESS, false);
        if (awsServerless) {
            if (awsOptionUsed) {
                throw new RuntimeException(String.format("%s option cannot be used along with %s option", AWS_SERVERLESS, AWS_OPTION));
            }
            builder.withAwsServerless(awsServerless);
        }

        final String documentRootKey = pluginSetting.getStringOrDefault(DOCUMENT_ROOT_KEY, null);
        builder.withDocumentRootKey(documentRootKey);

        return builder.build();
    }

    public IndexType getIndexType() {
        return indexType;
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

    public String getRoutingField() {
        return routingField;
    }

    public long getBulkSize() {
        return bulkSize;
    }

    public Optional<String> getIsmPolicyFile() {
        return ismPolicyFile;
    }

    public String getAction() {
        return action;
    }

    public String getS3AwsRegion() {
        return s3AwsRegion;
    }

    public String getS3AwsStsRoleArn() {
        return s3AwsStsRoleArn;
    }

    public boolean getAwsServerless() {
        return awsServerless;
    }

    public String getDocumentRootKey() {
        return documentRootKey;
    }

    /**
     * This method is used in the creation of IndexConfiguration object. It takes in the template file path
     * or index type and returns the index template read from the file or specific to index type or returns an
     * empty map.
     *
     * @param templateFile
     * @param indexType
     * @return
     */
    private Map<String, Object> readIndexTemplate(final String templateFile, final IndexType indexType) {
        try {
            URL templateURL = null;
            InputStream s3TemplateFile = null;
            if (indexType.equals(IndexType.TRACE_ANALYTICS_RAW)) {
                templateURL = getClass().getClassLoader()
                        .getResource(IndexConstants.RAW_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexType.TRACE_ANALYTICS_SERVICE_MAP)) {
                templateURL = getClass().getClassLoader()
                        .getResource(IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
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

    public static class Builder {
        private String indexAlias;
        private String indexType;
        private String templateFile;
        private int numShards;
        private int numReplicas;
        private String routingField;
        private String documentIdField;
        private long bulkSize = DEFAULT_BULK_SIZE;
        private Optional<String> ismPolicyFile;
        private String action;
        private String s3AwsRegion;
        private String s3AwsStsRoleArn;
        private S3Client s3Client;
        private boolean awsServerless;
        private String documentRootKey;

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

        public Builder withTemplateFile(final String templateFile) {
            checkArgument(templateFile != null, "templateFile cannot be null.");
            this.templateFile = templateFile;
            return this;
        }

        public Builder withDocumentIdField(final String documentIdField) {
            checkNotNull(documentIdField, "documentId field cannot be null");
            this.documentIdField = documentIdField;
            return this;
        }

        public Builder withRoutingField(final String routingField) {
            this.routingField = routingField;
            return this;
        }

        public Builder withBulkSize(final long bulkSize) {
            this.bulkSize = bulkSize;
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

        public Builder withAction(final String action) {
            checkArgument(EnumUtils.isValidEnumIgnoreCase(BulkAction.class, action), "action must be one of the following: " +  BulkAction.values());
            this.action = action;
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

        public Builder withS3Client(final S3Client s3Client) {
            checkArgument(s3Client != null);
            this.s3Client = s3Client;
            return this;
        }

        public Builder withAwsServerless(final boolean awsServerless) {
            this.awsServerless = awsServerless;
            return this;
        }

        public Builder withDocumentRootKey(final String documentRootKey) {
            if (documentRootKey != null) {
                checkArgument(!documentRootKey.isEmpty(), "documentRootKey cannot be empty string");
            }
            this.documentRootKey = documentRootKey;
            return this;
        }

        public IndexConfiguration build() {
            return new IndexConfiguration(this);
        }
    }
}
