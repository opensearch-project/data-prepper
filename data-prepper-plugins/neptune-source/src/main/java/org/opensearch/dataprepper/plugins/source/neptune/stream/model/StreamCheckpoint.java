/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@AllArgsConstructor
@Getter
public class StreamCheckpoint {
    private StreamPosition position;

    @Setter
    private long recordCount;

    public long getCommitNum() {
        return position.getCommitNum();
    }

    public long getOpNum() {
        return position.getOpNum();
    }

    public void setCommitNum(final long commitNum) {
        position.setCommitNum(commitNum);
    }

    public void setOpNum(final long opNum) {
        position.setOpNum(opNum);
    }

    public void resetPosition() {
        this.position = StreamPosition.empty();
    }

    public void incrementRecordCount() {
        recordCount++;
    }

    public static StreamCheckpoint emptyProgress() {
        return new StreamCheckpoint(new StreamPosition(0L, 0L), 0L);
    }

    public StreamCheckpoint(final StreamCheckpoint other) {
        this.position = new StreamPosition(other.getCommitNum(), other.getOpNum());
        this.recordCount = other.getRecordCount();
    }

    @Override
    public String toString() {
        return String.format("Checkpoint [pos=%s, recordCount=%s]", position, recordCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamCheckpoint that = (StreamCheckpoint) o;
        return recordCount == that.recordCount && Objects.equals(position, that.position);
    }
}
