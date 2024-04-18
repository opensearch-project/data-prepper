package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.stream.Collectors;

public class CollectionConfig {
    private static final String COLLECTION_SPLITTER = "\\.";
    private static final int DEFAULT_STREAM_BATCH_SIZE = 1000;
    private static final int DEFAULT_PARTITION_COUNT = 100;
    private static final int DEFAULT_EXPORT_BATCH_SIZE = 10_000;
    @JsonProperty("collection")
    private @NotNull String collection;

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

    @JsonProperty("partition_count")
    private int partitionCount;

    @JsonProperty("export_batch_size")
    private int exportBatchSize;
    @JsonProperty("stream_batch_size")
    private int streamBatchSize;

    public CollectionConfig() {
        this.export = true;
        this.stream = true;
        this.streamBatchSize = DEFAULT_STREAM_BATCH_SIZE;
        this.partitionCount = DEFAULT_PARTITION_COUNT;
        this.exportBatchSize = DEFAULT_EXPORT_BATCH_SIZE;
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

    public int getPartitionCount() {
        return this.partitionCount;
    }

    public int getExportBatchSize() {
        return this.exportBatchSize;
    }

    public int getStreamBatchSize() {
        return this.streamBatchSize;
    }
    public String getS3Region() {
        return this.s3Region;
    }
}
