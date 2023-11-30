/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class CheckpointStateTest {
    private static final int TEST_NUM_CHECKED_RECORDS = 3;

    @Test
    void testSimple() {
        final CheckpointState checkpointState = new CheckpointState(TEST_NUM_CHECKED_RECORDS);
        assertEquals(TEST_NUM_CHECKED_RECORDS, checkpointState.getNumRecordsToBeChecked());
    }
}