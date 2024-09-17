/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class KinesisCheckpointerTrackerTest {

    private Random random = new Random();

    @Test
    void testCheckPointerAddAndGet() {
        KinesisCheckpointerTracker kinesisCheckpointerTracker = new KinesisCheckpointerTracker();

        List<ExtendedSequenceNumber> extendedSequenceNumberList = new ArrayList<>();
        int numRecords = 10;
        for (int i=0; i<numRecords; i++) {
            RecordProcessorCheckpointer recordProcessorCheckpointer = mock(RecordProcessorCheckpointer.class);
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("seq-"+i, (long) i);
            extendedSequenceNumberList.add(extendedSequenceNumber);
            kinesisCheckpointerTracker.addRecordForCheckpoint(extendedSequenceNumber, recordProcessorCheckpointer);
        }

        ExtendedSequenceNumber last = extendedSequenceNumberList.get(extendedSequenceNumberList.size()-1);
        kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(last);

        Optional<KinesisCheckpointerRecord> checkpointRecord = kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord();
        assertTrue(checkpointRecord.isEmpty());
        assertEquals(kinesisCheckpointerTracker.size(), numRecords);

        int idx = random.nextInt(numRecords);
        ExtendedSequenceNumber extendedSequenceNumber1 = extendedSequenceNumberList.get(idx);
        kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(extendedSequenceNumber1);

        Optional<KinesisCheckpointerRecord> firstcheckpointer = kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord();
        if (idx != 0) {
            assertTrue(firstcheckpointer.isEmpty());
            assertEquals(kinesisCheckpointerTracker.size(), numRecords);
        } else {
            assertFalse(firstcheckpointer.isEmpty());
            assertEquals(kinesisCheckpointerTracker.size(), numRecords-1);
        }
    }
    @Test
    void testGetLastCheckpointerAndStoreIsEmpty() {
        KinesisCheckpointerTracker kinesisCheckpointerTracker = new KinesisCheckpointerTracker();

        List<ExtendedSequenceNumber> extendedSequenceNumberList = new ArrayList<>();
        int numRecords = 10;
        for (int i=0; i<numRecords; i++) {
            RecordProcessorCheckpointer recordProcessorCheckpointer = mock(RecordProcessorCheckpointer.class);
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("seq-"+i, (long) i);
            extendedSequenceNumberList.add(extendedSequenceNumber);
            kinesisCheckpointerTracker.addRecordForCheckpoint(extendedSequenceNumber, recordProcessorCheckpointer);
        }

        for (ExtendedSequenceNumber extendedSequenceNumber: extendedSequenceNumberList) {
            kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(extendedSequenceNumber);
        }

        Optional<KinesisCheckpointerRecord> checkpointer = kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord();
        assertTrue(checkpointer.isPresent());
        assertEquals(0, kinesisCheckpointerTracker.size());
    }

    @Test
    public void testMarkCheckpointerReadyForCheckpoint() {

        KinesisCheckpointerTracker kinesisCheckpointerTracker = new KinesisCheckpointerTracker();

        ExtendedSequenceNumber extendedSequenceNumber = mock(ExtendedSequenceNumber.class);
        assertThrows(IllegalArgumentException.class, () -> kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(extendedSequenceNumber));

        Optional<KinesisCheckpointerRecord> checkpointer = kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord();
        assertTrue(checkpointer.isEmpty());
    }
}
