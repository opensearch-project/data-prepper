package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class CollectionConfig {
    private static final String COLLECTION_SPLITTER = "\\.";
    @JsonProperty("collection")
    private @NotNull String collection;

    @JsonProperty("export_config")
    private ExportConfig exportConfig;

    @JsonProperty("ingestion_mode")
    private IngestionMode ingestionMode;

    public CollectionConfig() {
        this.ingestionMode = IngestionMode.EXPORT_STREAM;
        this.exportConfig = new ExportConfig();
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

    public IngestionMode getIngestionMode() {
        return this.ingestionMode;
    }

    public ExportConfig getExportConfig() {
        return this.exportConfig;
    }

    public boolean isExportRequired() {
        return this.ingestionMode == CollectionConfig.IngestionMode.EXPORT_STREAM ||
                this.ingestionMode == CollectionConfig.IngestionMode.EXPORT;
    }

    public boolean isStreamRequired() {
        return this.ingestionMode == CollectionConfig.IngestionMode.EXPORT_STREAM ||
                this.ingestionMode == CollectionConfig.IngestionMode.STREAM;
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

    public enum IngestionMode {
        EXPORT_STREAM("export_stream"),
        EXPORT("export"),
        STREAM("stream");

        private static final Map<String, IngestionMode> OPTIONS_MAP = Arrays.stream(values()).collect(Collectors.toMap((value) -> value.type, (value) -> value));
        private final String type;

        IngestionMode(String type) {
            this.type = type;
        }

        @JsonCreator
        public static IngestionMode fromTypeValue(String type) {
            return OPTIONS_MAP.get(type.toLowerCase());
        }
    }
}
