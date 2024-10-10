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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonInputConfig;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KinesisRecordConverterTest {
    private static final String streamId = "stream-1";

    @Test
    void testRecordConverter() throws IOException {
        InputCodec codec = mock(InputCodec.class);
        KinesisRecordConverter kinesisRecordConverter = new KinesisRecordConverter(codec);
        doNothing().when(codec).parse(any(InputStream.class), any(Consumer.class));

        String sample_record_data = "sample record data";
        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(sample_record_data.getBytes()))
                .build();
        kinesisRecordConverter.convert(List.of(kinesisClientRecord), streamId);
        verify(codec, times(1)).parse(any(InputStream.class), any(Consumer.class));
    }

    @Test
    public void testRecordConverterWithNdJsonInputCodec() throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();

        int numRecords = 10;
        final List<Map<String, Object>> jsonObjects = IntStream.range(0, numRecords)
                .mapToObj(i -> generateJson())
                .collect(Collectors.toList());

        final StringWriter writer = new StringWriter();

        for (final Map<String, Object> jsonObject : jsonObjects) {
            writer.append(objectMapper.writeValueAsString(jsonObject));
            writer.append(System.lineSeparator());
        }

        KinesisRecordConverter kinesisRecordConverter = new KinesisRecordConverter(
                new NdjsonInputCodec(new NdjsonInputConfig(), TestEventFactory.getTestEventFactory()));

        final String partitionKey = UUID.randomUUID().toString();
        final String sequenceNumber = UUID.randomUUID().toString();
        final Random random = new Random();
        final long subsequenceNumber = random.nextLong();

        KinesisClientRecord kinesisClientRecord = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(writer.toString().getBytes()))
                .sequenceNumber(sequenceNumber)
                .subSequenceNumber(subsequenceNumber)
                .partitionKey(partitionKey)
                .build();
        List<Record<Event>> events = kinesisRecordConverter.convert(List.of(kinesisClientRecord), streamId);

        assertEquals(events.size(), numRecords);
        events.forEach(eventRecord -> {
            assertEquals(eventRecord.getData().getMetadata().getAttribute(MetadataKeyAttributes.KINESIS_PARTITION_KEY_METADATA_ATTRIBUTE), partitionKey);
            assertEquals(eventRecord.getData().getMetadata().getAttribute(MetadataKeyAttributes.KINESIS_SEQUENCE_NUMBER_METADATA_ATTRIBUTE), sequenceNumber);
            assertEquals(eventRecord.getData().getMetadata().getAttribute(MetadataKeyAttributes.KINESIS_SUB_SEQUENCE_NUMBER_METADATA_ATTRIBUTE), subsequenceNumber);
        });
    }

    private static Map<String, Object> generateJson() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 1; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return jsonObject;
    }
}
