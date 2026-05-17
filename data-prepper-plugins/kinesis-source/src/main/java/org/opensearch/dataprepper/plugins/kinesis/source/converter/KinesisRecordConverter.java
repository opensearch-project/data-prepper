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

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisInputOutputRecord;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordConverter.class);
    static final String RECORD_PARSE_ERRORS = "recordParseErrors";
    private final InputCodec codec;
    private final Counter recordParseErrors;

    public KinesisRecordConverter(final InputCodec codec, final PluginMetrics pluginMetrics) {
        this.codec = codec;
        this.recordParseErrors = pluginMetrics.counter(RECORD_PARSE_ERRORS);
    }

    public List<KinesisInputOutputRecord> convert(final DecompressionEngine decompressionEngine,
                                             List<KinesisClientRecord> kinesisClientRecords,
                                             final String streamName) throws IOException {
        List<KinesisInputOutputRecord> records = new ArrayList<>();
        for (KinesisClientRecord kinesisClientRecord : kinesisClientRecords) {
            processRecord(decompressionEngine, kinesisClientRecord, record -> {
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
                records.add(KinesisInputOutputRecord.builder()
                        .withIncomingRecordSizeBytes(kinesisClientRecord.data().position())
                        .withDataPrepperRecord(record).build());
            });
        }
        return records;
    }

    private void processRecord(final DecompressionEngine decompressionEngine,
                               KinesisClientRecord record,
                               Consumer<Record<Event>> eventConsumer) throws IOException {
        // Read bytebuffer
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(arr);

        try {
            codec.parse(decompressionEngine.createInputStream(byteArrayInputStream), eventConsumer);
        } catch (final Exception e) {
            recordParseErrors.increment();
            LOG.error("Failed to parse Kinesis record. sequenceNumber={}, partitionKey={}",
                    record.sequenceNumber(), record.partitionKey(), e);
        }
    }
}
