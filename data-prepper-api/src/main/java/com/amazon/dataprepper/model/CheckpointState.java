package com.amazon.dataprepper.model;

public class CheckpointState {
    private final int numCheckedRecords;

    public CheckpointState(final int numCheckedRecords) {
        this.numCheckedRecords = numCheckedRecords;
    }

    public int getNumCheckedRecords() {
        return numCheckedRecords;
    }
}
