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
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordConverter {

    private final InputCodec codec;

    public KinesisRecordConverter(final InputCodec codec) {
        this.codec = codec;
    }

    public List<Record<Event>> convert(List<KinesisClientRecord> kinesisClientRecords,
                                       final String streamName) throws IOException {
        List<Record<Event>> records = new ArrayList<>();
        for (KinesisClientRecord kinesisClientRecord : kinesisClientRecords) {
            processRecord(kinesisClientRecord, record -> {
                records.add(record);
                Event event = record.getData();
                EventMetadata eventMetadata = event.getMetadata();
                eventMetadata.setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE,
                        streamName.toLowerCase());
                eventMetadata.setAttribute(MetadataKeyAttributes.KINESIS_PARTITION_KEY_METADATA_ATTRIBUTE, kinesisClientRecord.partitionKey());
                eventMetadata.setAttribute(MetadataKeyAttributes.KINESIS_SEQUENCE_NUMBER_METADATA_ATTRIBUTE, kinesisClientRecord.sequenceNumber());
                eventMetadata.setAttribute(MetadataKeyAttributes.KINESIS_SUB_SEQUENCE_NUMBER_METADATA_ATTRIBUTE, kinesisClientRecord.subSequenceNumber());
                final Instant externalOriginationTime = kinesisClientRecord.approximateArrivalTimestamp();
                event.getEventHandle().setExternalOriginationTime(externalOriginationTime);
                event.getMetadata().setExternalOriginationTime(externalOriginationTime);
            });
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
