/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class CheckpointEntry {

    private long readOffset;
    private long committedOffset;
    private CheckpointStatus status;
    private long lastUpdatedMillis;

    @JsonCreator
    public CheckpointEntry(
            @JsonProperty("readOffset") final long readOffset,
            @JsonProperty("committedOffset") final long committedOffset,
            @JsonProperty("status") final CheckpointStatus status,
            @JsonProperty("lastUpdatedMillis") final long lastUpdatedMillis) {
        this.readOffset = readOffset;
        this.committedOffset = committedOffset;
        this.status = status;
        this.lastUpdatedMillis = lastUpdatedMillis;
    }

    public CheckpointEntry(final long readOffset, final long committedOffset, final CheckpointStatus status) {
        this(readOffset, committedOffset, status, System.currentTimeMillis());
    }

    public CheckpointEntry() {
        this(0, 0, CheckpointStatus.ACTIVE);
    }

    public synchronized long getReadOffset() {
        return readOffset;
    }

    public synchronized void setReadOffset(final long readOffset) {
        this.readOffset = readOffset;
        this.lastUpdatedMillis = System.currentTimeMillis();
    }

    public synchronized long getCommittedOffset() {
        return committedOffset;
    }

    public synchronized void setCommittedOffset(final long committedOffset) {
        this.committedOffset = committedOffset;
        this.lastUpdatedMillis = System.currentTimeMillis();
    }

    public synchronized CheckpointStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(final CheckpointStatus status) {
        this.status = status;
        this.lastUpdatedMillis = System.currentTimeMillis();
    }

    public synchronized long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }

    public synchronized CheckpointEntry snapshot() {
        return new CheckpointEntry(readOffset, committedOffset, status, lastUpdatedMillis);
    }
}
