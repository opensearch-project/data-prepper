package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoDBSnapshotProgressState {
    @JsonProperty("totalRecords")
    private long total;
    @JsonProperty("successRecords")
    private long success;
    @JsonProperty("failedRecords")
    private long failed;

    public long getTotal() {
        return total;
    }

    public long getSuccess() {
        return success;
    }

    public long getFailed() {
        return failed;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void setSuccess(long successRecords) {
        this.success = successRecords;
    }

    public void setFailure(long failedRecords) {
        this.failed = failedRecords;
    }
}
