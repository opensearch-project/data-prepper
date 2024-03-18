/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExportProgressState {

    @JsonProperty("databaseName")
    private String databaseName;

    @JsonProperty("collectionName")
    private String collectionName;

    @JsonProperty("exportTime")
    private String exportTime;

    @JsonProperty("status")
    private String status;


    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getExportTime() {
        return exportTime;
    }

    public void setExportTime(String exportTime) {
        this.exportTime = exportTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
