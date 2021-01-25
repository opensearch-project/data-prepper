package com.amazon.dataprepper.model;

/**
 * CheckpointState keeps track of a summary of records processed by a data-prepper worker thread.
 */
public class CheckpointState {
    private final int numCheckedRecords;

    public CheckpointState(final int numCheckedRecords) {
        this.numCheckedRecords = numCheckedRecords;
    }

    public int getNumCheckedRecords() {
        return numCheckedRecords;
    }
}
