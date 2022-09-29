/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model;

/**
 * CheckpointState keeps track of a summary of records processed by a data-prepper worker thread.
 */
public class CheckpointState {
    private final int numRecordsToBeChecked;

    public CheckpointState(final int numRecordsToBeChecked) {
        this.numRecordsToBeChecked = numRecordsToBeChecked;
    }

    public int getNumRecordsToBeChecked() {
        return numRecordsToBeChecked;
    }
}
