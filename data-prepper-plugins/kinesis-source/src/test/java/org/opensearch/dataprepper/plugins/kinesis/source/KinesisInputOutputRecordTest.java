/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.MetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisInputOutputRecord;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KinesisInputOutputRecordTest {

    @Test
    void builder_defaultCreatesObjectCorrectly() {

        KinesisInputOutputRecord kinesisInputOutputRecord = KinesisInputOutputRecord.builder().build();

        assertNull(kinesisInputOutputRecord.getKinesisClientRecord());
        assertNull(kinesisInputOutputRecord.getDataPrepperRecord());
    }

    @Test
    void builder_createsObjectCorrectly() {
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        event.getMetadata().setAttribute(MetadataKeyAttributes.KINESIS_STREAM_NAME_METADATA_ATTRIBUTE, UUID.randomUUID().toString());
        Record<Event> record = new Record<>(event);
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(event.toJsonString().getBytes()))
                .sequenceNumber(Integer.toString(1000)).subSequenceNumber(1).build();

        KinesisInputOutputRecord kinesisInputOutputRecord = KinesisInputOutputRecord.builder()
                .withKinesisClientRecord(kinesisClientRecord)
                .withDataPrepperRecord(record)
                .build();

        assertNotNull(kinesisInputOutputRecord.getKinesisClientRecord());
        assertNotNull(kinesisInputOutputRecord.getDataPrepperRecord());
        assertEquals(kinesisInputOutputRecord.getDataPrepperRecord(), record);
        assertEquals(kinesisInputOutputRecord.getKinesisClientRecord(), kinesisClientRecord);
    }
}
