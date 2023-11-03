package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoDBSnapshotProgressState {
    @JsonProperty("totalRecords")
    private int total;

    public int getTotal() {
        return total;
    }


    public void setTotal(int total) {
        this.total = total;
    }

}
