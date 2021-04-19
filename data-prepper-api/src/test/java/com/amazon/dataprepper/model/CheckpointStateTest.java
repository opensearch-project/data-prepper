/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointStateTest {
    private static final int TEST_NUM_CHECKED_RECORDS = 3;

    @Test
    public void testSimple() {
        final CheckpointState checkpointState = new CheckpointState(TEST_NUM_CHECKED_RECORDS);
        assertEquals(TEST_NUM_CHECKED_RECORDS, checkpointState.getNumRecordsToBeChecked());
    }
}