/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ShuffleWriteProgressState {

    @JsonProperty("snapshotId")
    private long snapshotId;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("dataFilePath")
    private String dataFilePath;

    @JsonProperty("taskType")
    private String taskType;

    @JsonProperty("shuffleTaskId")
    private String shuffleTaskId;

    @JsonProperty("nodeAddress")
    private String nodeAddress;

    @JsonProperty("changeOrdinal")
    private int changeOrdinal;

    public long getSnapshotId() { return snapshotId; }
    public void setSnapshotId(final long snapshotId) { this.snapshotId = snapshotId; }

    public String getTableName() { return tableName; }
    public void setTableName(final String tableName) { this.tableName = tableName; }

    public String getDataFilePath() { return dataFilePath; }
    public void setDataFilePath(final String dataFilePath) { this.dataFilePath = dataFilePath; }

    public String getTaskType() { return taskType; }
    public void setTaskType(final String taskType) { this.taskType = taskType; }

    public String getShuffleTaskId() { return shuffleTaskId; }
    public void setShuffleTaskId(final String shuffleTaskId) { this.shuffleTaskId = shuffleTaskId; }

    public String getNodeAddress() { return nodeAddress; }
    public void setNodeAddress(final String nodeAddress) { this.nodeAddress = nodeAddress; }

    public int getChangeOrdinal() { return changeOrdinal; }
    public void setChangeOrdinal(final int changeOrdinal) { this.changeOrdinal = changeOrdinal; }
}
