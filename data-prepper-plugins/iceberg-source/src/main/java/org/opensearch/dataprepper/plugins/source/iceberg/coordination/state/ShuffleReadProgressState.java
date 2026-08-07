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

import java.util.List;

public class ShuffleReadProgressState {

    @JsonProperty("snapshotId")
    private long snapshotId;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("partitionRangeStart")
    private int partitionRangeStart;

    @JsonProperty("partitionRangeEnd")
    private int partitionRangeEnd;

    @JsonProperty("shuffleWriteTaskIds")
    private List<String> shuffleWriteTaskIds;

    @JsonProperty("nodeAddresses")
    private List<String> nodeAddresses;

    public long getSnapshotId() { return snapshotId; }
    public void setSnapshotId(final long snapshotId) { this.snapshotId = snapshotId; }

    public String getTableName() { return tableName; }
    public void setTableName(final String tableName) { this.tableName = tableName; }

    public int getPartitionRangeStart() { return partitionRangeStart; }
    public void setPartitionRangeStart(final int partitionRangeStart) { this.partitionRangeStart = partitionRangeStart; }

    public int getPartitionRangeEnd() { return partitionRangeEnd; }
    public void setPartitionRangeEnd(final int partitionRangeEnd) { this.partitionRangeEnd = partitionRangeEnd; }

    public List<String> getShuffleWriteTaskIds() { return shuffleWriteTaskIds; }
    public void setShuffleWriteTaskIds(final List<String> shuffleWriteTaskIds) { this.shuffleWriteTaskIds = shuffleWriteTaskIds; }

    public List<String> getNodeAddresses() { return nodeAddresses; }
    public void setNodeAddresses(final List<String> nodeAddresses) { this.nodeAddresses = nodeAddresses; }
}
