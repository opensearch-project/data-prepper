/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.converter;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordConverter {

    private final InputCodec codec;

    public KinesisRecordConverter(final InputCodec codec) {
        this.codec = codec;
    }

    public List<Record<Event>> convert(List<KinesisClientRecord> kinesisClientRecords) throws IOException {
        List<Record<Event>> records = new ArrayList<>();
        for (KinesisClientRecord record : kinesisClientRecords) {
            processRecord(record, records::add);
        }
        return records;
    }

    private void processRecord(KinesisClientRecord record, Consumer<Record<Event>> eventConsumer) throws IOException {
        // Read bytebuffer
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(arr);
        codec.parse(byteArrayInputStream, eventConsumer);
    }
}
