/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;

import java.io.InputStream;
import java.util.UUID;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataFileLoaderTest {

    @Mock
    private DataFilePartition dataFilePartition;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private InputCodec codec;

    @Mock
    private S3ObjectReader s3ObjectReader;

    @Mock
    private ExportRecordConverter recordConverter;

    @Test
    void test_run() throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();
        when(dataFilePartition.getBucket()).thenReturn(bucket);
        when(dataFilePartition.getKey()).thenReturn(key);

        InputStream inputStream = mock(InputStream.class);
        when(s3ObjectReader.readFile(bucket, key)).thenReturn(inputStream);

        DataFileLoader objectUnderTest = createObjectUnderTest();
        objectUnderTest.run();

        verify(codec).parse(eq(inputStream), any(Consumer.class));
        verify(bufferAccumulator).flush();
    }

    private DataFileLoader createObjectUnderTest() {
        return DataFileLoader.create(dataFilePartition, codec, bufferAccumulator, s3ObjectReader, recordConverter);
    }
}