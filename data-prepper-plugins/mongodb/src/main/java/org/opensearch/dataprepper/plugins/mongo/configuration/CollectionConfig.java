package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Arrays;
import java.util.stream.Collectors;

public class CollectionConfig {
    private static final String COLLECTION_SPLITTER = "\\.";
    private static final int DEFAULT_STREAM_BATCH_SIZE = 1000;
    private static final int DEFAULT_PARTITION_COUNT = 100;
    private static final int DEFAULT_EXPORT_BATCH_SIZE = 10_000;
    @JsonProperty("collection")
    @Pattern(regexp = ".+?\\..+", message = "Should be of pattern <database>.<collection>")
    private @NotNull String collection;

    @JsonProperty("export")
    private boolean export;

    @JsonProperty("stream")
    private boolean stream;

    @JsonProperty("partition_count")
    @Min(1)
    @Max(1000)
    private int partitionCount;

    @JsonProperty("export_batch_size")
    @Min(1)
    @Max(1_000_000)
    private int exportBatchSize;
    @JsonProperty("stream_batch_size")
    @Min(1)
    @Max(1_000_000)
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

    public int getPartitionCount() {
        return this.partitionCount;
    }

    public int getExportBatchSize() {
        return this.exportBatchSize;
    }

    public int getStreamBatchSize() {
        return this.streamBatchSize;
    }
}
