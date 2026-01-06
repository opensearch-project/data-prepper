/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
