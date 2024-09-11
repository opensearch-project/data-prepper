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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class KinesisCheckpointerRecordTest {
    private String shardId = "shardId-123";
    private String testConcurrencyToken = "testToken";

    @Test
    public void validateTwoRecords() {

        KinesisCheckpointerRecord kinesisCheckpointerRecord1 = KinesisCheckpointerRecord.builder()
                .extendedSequenceNumber(ExtendedSequenceNumber.LATEST)
                .readyToCheckpoint(false)
                .build();
        KinesisCheckpointerRecord kinesisCheckpointerRecord2 = KinesisCheckpointerRecord.builder()
                .extendedSequenceNumber(ExtendedSequenceNumber.LATEST)
                .readyToCheckpoint(false)
                .build();

        assertEquals(kinesisCheckpointerRecord1.isReadyToCheckpoint(), kinesisCheckpointerRecord2.isReadyToCheckpoint());
        assertEquals(kinesisCheckpointerRecord1.getCheckpointer(), kinesisCheckpointerRecord2.getCheckpointer());
        assertEquals(kinesisCheckpointerRecord1.getExtendedSequenceNumber(), kinesisCheckpointerRecord2.getExtendedSequenceNumber());
    }

    @Test
    public void validateTwoRecordsWithSetterMethods() {
        RecordProcessorCheckpointer recordProcessorCheckpointer = mock(RecordProcessorCheckpointer.class);
        KinesisCheckpointerRecord kinesisCheckpointerRecord1 = KinesisCheckpointerRecord.builder().build();
        kinesisCheckpointerRecord1.setCheckpointer(recordProcessorCheckpointer);
        kinesisCheckpointerRecord1.setExtendedSequenceNumber(ExtendedSequenceNumber.LATEST);
        kinesisCheckpointerRecord1.setReadyToCheckpoint(false);

        KinesisCheckpointerRecord kinesisCheckpointerRecord2 = KinesisCheckpointerRecord.builder().build();
        kinesisCheckpointerRecord2.setCheckpointer(recordProcessorCheckpointer);
        kinesisCheckpointerRecord2.setExtendedSequenceNumber(ExtendedSequenceNumber.LATEST);
        kinesisCheckpointerRecord2.setReadyToCheckpoint(false);

        assertEquals(kinesisCheckpointerRecord1.isReadyToCheckpoint(), kinesisCheckpointerRecord2.isReadyToCheckpoint());
        assertEquals(kinesisCheckpointerRecord1.getCheckpointer(), kinesisCheckpointerRecord2.getCheckpointer());
        assertEquals(kinesisCheckpointerRecord1.getExtendedSequenceNumber(), kinesisCheckpointerRecord2.getExtendedSequenceNumber());
    }

    @Test
    public void testInvalidRecords() {
        KinesisCheckpointerRecord kinesisCheckpointerRecord = KinesisCheckpointerRecord.builder().build();
        assertNotNull(kinesisCheckpointerRecord);
    }
}
