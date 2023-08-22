package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.commons.lang3.Range;

public class CommitOffsetRange {
    private final long epoch;
    private final Range<Long> offsets;

    public CommitOffsetRange(final Range<Long> offsets, final long epoch) {
        this.offsets = offsets;
        this.epoch = epoch;
    }

    public long getEpoch() {
        return epoch;
    }

    public Range<Long> getOffsets() {
        return offsets;
    }
}
