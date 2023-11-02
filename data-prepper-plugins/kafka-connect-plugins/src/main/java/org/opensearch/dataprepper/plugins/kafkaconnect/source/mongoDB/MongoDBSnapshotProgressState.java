package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoDBSnapshotProgressState {
    @JsonProperty("totalRecords")
    private int total;

    @JsonProperty("loadedRecords")
    private int loaded;

    public int getTotal() {
        return total;
    }

    public int getLoaded() {
        return loaded;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public void setLoaded(int loaded) {
        this.loaded = loaded;
    }
}
