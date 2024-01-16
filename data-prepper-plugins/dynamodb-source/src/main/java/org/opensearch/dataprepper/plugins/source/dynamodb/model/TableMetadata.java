/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamStartPosition;

import java.util.HashMap;
import java.util.Map;

public class TableMetadata {

    private static final String PARTITION_KEY = "partitionKey";
    private static final String SORT_KEY = "sortKey";

    private static final String STREAM_ARN_KEY = "streamArn";
    private static final String REQUIRE_EXPORT_KEY = "export";
    private static final String REQUIRE_STREAM_KEY = "stream";

    private final String partitionKeyAttributeName;

    private final String sortKeyAttributeName;

    private final StreamStartPosition streamStartPosition;

    private final String streamArn;

    private final boolean streamRequired;

    private final boolean exportRequired;

    private final String exportBucket;

    private final String exportPrefix;

    private final String exportKmsKeyId;

    private TableMetadata(Builder builder) {
        this.partitionKeyAttributeName = builder.partitionKeyAttributeName;
        this.sortKeyAttributeName = builder.sortKeyAttributeName;
        this.streamArn = builder.streamArn;
        this.streamRequired = builder.streamRequired;
        this.exportRequired = builder.exportRequired;
        this.exportBucket = builder.exportBucket;
        this.exportPrefix = builder.exportPrefix;
        this.streamStartPosition = builder.streamStartPosition;
        this.exportKmsKeyId = builder.exportKmsKeyId;

    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {


        private String partitionKeyAttributeName;

        private String sortKeyAttributeName;

        private boolean streamRequired;

        private boolean exportRequired;

        private String streamArn;

        private String exportBucket;

        private String exportPrefix;

        private String exportKmsKeyId;

        private StreamStartPosition streamStartPosition;


        public Builder partitionKeyAttributeName(String partitionKeyAttributeName) {
            this.partitionKeyAttributeName = partitionKeyAttributeName;
            return this;
        }

        public Builder sortKeyAttributeName(String sortKeyAttributeName) {
            this.sortKeyAttributeName = sortKeyAttributeName;
            return this;
        }

        public Builder streamArn(String streamArn) {
            this.streamArn = streamArn;
            return this;
        }

        public Builder streamRequired(boolean streamRequired) {
            this.streamRequired = streamRequired;
            return this;
        }

        public Builder exportRequired(boolean exportRequired) {
            this.exportRequired = exportRequired;
            return this;
        }

        public Builder exportBucket(String exportBucket) {
            this.exportBucket = exportBucket;
            return this;
        }

        public Builder exportPrefix(String exportPrefix) {
            this.exportPrefix = exportPrefix;
            return this;
        }

        public Builder exportKmsKeyId(String exportKmsKeyId) {
            this.exportKmsKeyId = exportKmsKeyId;
            return this;
        }

        public Builder streamStartPosition(StreamStartPosition streamStartPosition) {
            this.streamStartPosition = streamStartPosition;
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(this);
        }

    }


    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(PARTITION_KEY, partitionKeyAttributeName);
        map.put(SORT_KEY, sortKeyAttributeName);
        map.put(STREAM_ARN_KEY, streamArn);
        map.put(REQUIRE_EXPORT_KEY, exportRequired);
        map.put(REQUIRE_STREAM_KEY, streamRequired);
        return map;

    }

    public static TableMetadata fromMap(Map<String, Object> map) {
        return TableMetadata.builder()
                .partitionKeyAttributeName((String) map.get(PARTITION_KEY))
                .sortKeyAttributeName((String) map.get(SORT_KEY))
                .streamArn((String) map.get(STREAM_ARN_KEY))
                .exportRequired((boolean) map.getOrDefault(REQUIRE_EXPORT_KEY, false))
                .streamRequired((boolean) map.getOrDefault(REQUIRE_STREAM_KEY, false))
                .build();
    }


    public String getPartitionKeyAttributeName() {
        return partitionKeyAttributeName;
    }

    public String getSortKeyAttributeName() {
        return sortKeyAttributeName;
    }

    public String getStreamArn() {
        return streamArn;
    }

    public boolean isStreamRequired() {
        return streamRequired;
    }

    public boolean isExportRequired() {
        return exportRequired;
    }

    public StreamStartPosition getStreamStartPosition() {
        return streamStartPosition;
    }

    public String getExportBucket() {
        return exportBucket;
    }

    public String getExportPrefix() {
        return exportPrefix;
    }

    public String getExportKmsKeyId() {
        return exportKmsKeyId;
    }
}
