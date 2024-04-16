package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.stream.Collectors;

public class CollectionConfig {
    private static final String COLLECTION_SPLITTER = "\\.";
    private static final int DEFAULT_STREAM_BATCH_SIZE = 1000;
    @JsonProperty("collection")
    private @NotNull String collection;

    @JsonProperty("export_config")
    private ExportConfig exportConfig;

    @JsonProperty("export")
    private boolean export;

    @JsonProperty("stream")
    private boolean stream;

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_path_prefix")
    private String s3PathPrefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("stream_batch_size")
    private int streamBatchSize;

    public CollectionConfig() {
        this.export = true;
        this.stream = true;
        this.exportConfig = new ExportConfig();
        this.streamBatchSize = DEFAULT_STREAM_BATCH_SIZE;
    }

    public String getCollection() {
        return this.collection;
    }

    public String getDatabaseName() {
        return Arrays.stream(collection.split(COLLECTION_SPLITTER)).collect(Collectors.toList()).get(0);
    }

    public String getCollectionName() {
        return Arrays.stream(collection.split(COLLECTION_SPLITTER)).collect(Collectors.toList()).get(1);
    }

    public boolean isExport() {
        return this.export;
    }

    public boolean isStream() {
        return this.stream;
    }

    public String getS3Bucket() {
        return this.s3Bucket;
    }

    public String getS3PathPrefix() {
        return this.s3PathPrefix;
    }

    public int getStreamBatchSize() {
        return this.streamBatchSize;
    }
    public String getS3Region() {
        return this.s3Region;
    }

    public ExportConfig getExportConfig() {
        return this.exportConfig;
    }

    public static class ExportConfig {
        private static final int DEFAULT_ITEMS_PER_PARTITION = 4000;
        @JsonProperty("items_per_partition")
        private Integer itemsPerPartition;

        public ExportConfig() {
            this.itemsPerPartition = DEFAULT_ITEMS_PER_PARTITION;
        }

        public Integer getItemsPerPartition() {
            return this.itemsPerPartition;
        }
    }
}
