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

import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KinesisCheckpointerTracker {
    private final Map<ExtendedSequenceNumber, KinesisCheckpointerRecord> checkpointerRecordList = new LinkedHashMap<>();

    public synchronized void addRecordForCheckpoint(final ExtendedSequenceNumber extendedSequenceNumber,
                                                    final RecordProcessorCheckpointer checkpointer) {
        checkpointerRecordList.put(extendedSequenceNumber, KinesisCheckpointerRecord.builder()
                .extendedSequenceNumber(extendedSequenceNumber)
                .checkpointer(checkpointer)
                .readyToCheckpoint(false)
                .build());
    }

    public synchronized void markSequenceNumberForCheckpoint(final ExtendedSequenceNumber extendedSequenceNumber) {
        if (!checkpointerRecordList.containsKey(extendedSequenceNumber)) {
            throw new IllegalArgumentException("checkpointer not available");
        }
        checkpointerRecordList.get(extendedSequenceNumber).setReadyToCheckpoint(true);
    }

    public synchronized Optional<KinesisCheckpointerRecord> getLatestAvailableCheckpointRecord() {
        Optional<KinesisCheckpointerRecord> kinesisCheckpointerRecordOptional = Optional.empty();
        List<ExtendedSequenceNumber> toRemoveRecords = new ArrayList<>();

        for (Map.Entry<ExtendedSequenceNumber, KinesisCheckpointerRecord> entry: checkpointerRecordList.entrySet()) {
            KinesisCheckpointerRecord kinesisCheckpointerRecord = entry.getValue();

            // Break out of the loop on the first record which is not ready for checkpoint
            if (!kinesisCheckpointerRecord.isReadyToCheckpoint()) {
                break;
            }

            kinesisCheckpointerRecordOptional = Optional.of(kinesisCheckpointerRecord);
            toRemoveRecords.add(entry.getKey());
        }

        //Cleanup the ones which are already marked for checkpoint
        for (ExtendedSequenceNumber extendedSequenceNumber: toRemoveRecords) {
            checkpointerRecordList.remove(extendedSequenceNumber);
        }

        return kinesisCheckpointerRecordOptional;
    }

    public synchronized int size() {
        return checkpointerRecordList.size();
    }
}
