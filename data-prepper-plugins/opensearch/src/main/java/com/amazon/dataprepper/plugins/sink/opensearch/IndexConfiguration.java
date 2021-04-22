/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IndexConfiguration {
    /**
     * TODO: add index management policy parameters
     */
    public static final String SETTINGS = "settings";
    public static final String TRACE_ANALYTICS_RAW_FLAG = "trace_analytics_raw";
    public static final String TRACE_ANALYTICS_SERVICE_MAP_FLAG = "trace_analytics_service_map";
    public static final String INDEX_ALIAS = "index";
    public static final String TEMPLATE_FILE = "template_file";
    public static final String NUM_SHARDS = "number_of_shards";
    public static final String NUM_REPLICAS = "number_of_replicas";
    public static final String BULK_SIZE = "bulk_size";
    public static final String DOCUMENT_ID_FIELD = "document_id_field";
    public static final long DEFAULT_BULK_SIZE = 5L;

    private final String indexType;
    private final String indexAlias;
    private final Map<String, Object> indexTemplate;
    private final String documentIdField;
    private final long bulkSize;

    @SuppressWarnings("unchecked")
    private IndexConfiguration(final Builder builder) {
        if (builder.isRaw && builder.isServiceMap) {
            throw new IllegalStateException("trace_analytics_raw and trace_analytics_service_map cannot be both true.");
        } else if (builder.isRaw) {
            this.indexType  = IndexConstants.RAW;
        } else if (builder.isServiceMap) {
            this.indexType  = IndexConstants.SERVICE_MAP;
        } else {
            this.indexType  = IndexConstants.CUSTOM;
        }

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

        String documentIdField = builder.documentIdField;
        if (indexType.equals(IndexConstants.RAW)) {
            documentIdField = "spanId";
        } else if (indexType.equals(IndexConstants.SERVICE_MAP)) {
            documentIdField = "hashId";
        }
        this.documentIdField = documentIdField;
    }

    public static IndexConfiguration readIndexConfig(final PluginSetting pluginSetting) {
        IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
        builder.setIsRaw(pluginSetting.getBooleanOrDefault(TRACE_ANALYTICS_RAW_FLAG, false));
        builder.setIsServiceMap(pluginSetting.getBooleanOrDefault(TRACE_ANALYTICS_SERVICE_MAP_FLAG, false));
        final String indexAlias = pluginSetting.getStringOrDefault(INDEX_ALIAS, null);
        if (indexAlias != null) {
            builder = builder.withIndexAlias(indexAlias);
        }
        final String templateFile = pluginSetting.getStringOrDefault(TEMPLATE_FILE, null);
        if (templateFile != null) {
            builder = builder.withTemplateFile(templateFile);
        }
        builder = builder.withNumShards(pluginSetting.getIntegerOrDefault(NUM_SHARDS, 0));
        builder = builder.withNumShards(pluginSetting.getIntegerOrDefault(NUM_REPLICAS, 0));
        final Long batchSize = pluginSetting.getLongOrDefault(BULK_SIZE, DEFAULT_BULK_SIZE);
        builder = builder.withBulkSize(batchSize);
        final String documentId = pluginSetting.getStringOrDefault(DOCUMENT_ID_FIELD, null);
        if (documentId != null) {
            builder = builder.withDocumentIdField(documentId);
        }
        return builder.build();
    }

    public String getIndexType() {
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

    public long getBulkSize() {
        return bulkSize;
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
    private Map<String, Object> readIndexTemplate(final String templateFile, final String indexType) {
        try {
            URL templateURL = null;
            if (indexType.equals(IndexConstants.RAW)) {
                templateURL = getClass().getClassLoader()
                        .getResource(IndexConstants.RAW_DEFAULT_TEMPLATE_FILE);
            } else if (indexType.equals(IndexConstants.SERVICE_MAP)) {
                templateURL = getClass().getClassLoader()
                        .getResource(IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
            } else if (templateFile != null) {
                templateURL = new File(templateFile).toURI().toURL();
            }
            if (templateURL != null) {
                return new ObjectMapper().readValue(templateURL, new TypeReference<Map<String, Object>>() {
                });
            } else {
                return new HashMap<>();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Index template is not valid.", ex);
        }
    }

    public static class Builder {
        private boolean isRaw = false;
        private boolean isServiceMap = false;
        private String indexAlias;
        private String templateFile;
        private int numShards;
        private int numReplicas;
        private String documentIdField;
        private long bulkSize = DEFAULT_BULK_SIZE;

        public Builder setIsRaw(final Boolean isRaw) {
            checkNotNull(isRaw, "trace_analytics_raw cannot be null.");
            this.isRaw = isRaw;
            return this;
        }

        public Builder setIsServiceMap(final Boolean isServiceMap) {
            checkNotNull(isServiceMap, "trace_analytics_service_map cannot be null.");
            this.isServiceMap = isServiceMap;
            return this;
        }

        public Builder withIndexAlias(final String indexAlias) {
            checkArgument(indexAlias != null, "indexAlias cannot be null.");
            checkArgument(!indexAlias.isEmpty(), "indexAlias cannot be empty");
            this.indexAlias = indexAlias;
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

        public IndexConfiguration build() {
            return new IndexConfiguration(this);
        }
    }
}
