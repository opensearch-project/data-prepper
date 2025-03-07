/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;

import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileLoader.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileLoader.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileLoader.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileLoader.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileLoader.EXPORT_RECORDS_TOTAL_COUNT;

@ExtendWith(MockitoExtension.class)
class DataFileLoaderTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DataFilePartition dataFilePartition;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private S3ObjectReader s3ObjectReader;

    @Mock
    private ExportRecordConverter recordConverter;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private DbTableMetadata dbTableMetadata;

    @Mock
    private Duration acknowledgmentTimeout;

    @Mock
    private Counter exportRecordsTotalCounter;

    @Mock
    private Counter exportRecordSuccessCounter;

    @Mock
    private Counter exportRecordErrorCounter;

    @Mock
    private DistributionSummary bytesReceivedSummary;

    @Mock
    private DistributionSummary bytesProcessedSummary;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT)).thenReturn(exportRecordsTotalCounter);
        when(pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT)).thenReturn(exportRecordSuccessCounter);
        when(pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT)).thenReturn(exportRecordErrorCounter);
        when(pluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        when(pluginMetrics.summary(BYTES_PROCESSED)).thenReturn(bytesProcessedSummary);
    }

    @ParameterizedTest
    @EnumSource(EngineType.class)
    void test_run_success(EngineType engineType) throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();
        when(dataFilePartition.getBucket()).thenReturn(bucket);
        when(dataFilePartition.getKey()).thenReturn(key);
        final DataFileProgressState progressState = mock(DataFileProgressState.class, RETURNS_DEEP_STUBS);
        when(dataFilePartition.getProgressState()).thenReturn(Optional.of(progressState));
        when(progressState.getEngineType()).thenReturn(engineType.toString());

        InputStream inputStream = mock(InputStream.class);
        when(s3ObjectReader.readFile(bucket, key)).thenReturn(inputStream);

        DataFileLoader dataFileLoader = createObjectUnderTest();

        final String randomString = UUID.randomUUID().toString();
        final long sizeBytes = randomString.getBytes().length;
        final BaseEventBuilder<Event> eventBuilder = mock(EventBuilder.class, RETURNS_DEEP_STUBS);
        final Event event = mock(Event.class);
        when(eventFactory.eventBuilder(any())).thenReturn(eventBuilder);
        when(eventBuilder.withEventType(any()).withData(any()).build()).thenReturn(event);
        when(event.toJsonString()).thenReturn(randomString);
        when(recordConverter.convert(any(), any(), any(), any(), any(), any(), anyLong(), anyLong(), any())).thenReturn(event);

        AvroParquetReader.Builder<GenericRecord> builder = mock(AvroParquetReader.Builder.class);
        ParquetReader<GenericRecord> parquetReader = mock(ParquetReader.class);
        BufferAccumulator<Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        when(builder.build()).thenReturn(parquetReader);
        when(parquetReader.read()).thenReturn(mock(GenericRecord.class, RETURNS_DEEP_STUBS), (GenericRecord) null);
        try (MockedStatic<AvroParquetReader> readerMockedStatic = mockStatic(AvroParquetReader.class);
             MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {

            readerMockedStatic.when(() -> AvroParquetReader.<GenericRecord>builder(any(InputFile.class), any())).thenReturn(builder);
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(any(Buffer.class), anyInt(), any(Duration.class))).thenReturn(bufferAccumulator);

            dataFileLoader.run();
        }

        verify(bufferAccumulator).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(acknowledgementSet).add(event);
        verify(acknowledgementSet).complete();

        verify(exportRecordsTotalCounter).increment();
        verify(bytesReceivedSummary).record(sizeBytes);
        verify(bytesProcessedSummary).record(sizeBytes);
        verify(exportRecordSuccessCounter).increment(1);
        verify(exportRecordErrorCounter, never()).increment(1);
    }

    @ParameterizedTest
    @EnumSource(EngineType.class)
    void test_flush_failure_then_error_metric_updated(EngineType engineType) throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();
        when(dataFilePartition.getBucket()).thenReturn(bucket);
        when(dataFilePartition.getKey()).thenReturn(key);
        final DataFileProgressState progressState = mock(DataFileProgressState.class, RETURNS_DEEP_STUBS);
        when(dataFilePartition.getProgressState()).thenReturn(Optional.of(progressState));
        when(progressState.getEngineType()).thenReturn(engineType.toString());

        InputStream inputStream = mock(InputStream.class);
        when(s3ObjectReader.readFile(bucket, key)).thenReturn(inputStream);

        DataFileLoader dataFileLoader = createObjectUnderTest();

        final String randomString = UUID.randomUUID().toString();
        final long sizeBytes = randomString.getBytes().length;
        final BaseEventBuilder<Event> eventBuilder = mock(EventBuilder.class, RETURNS_DEEP_STUBS);
        final Event event = mock(Event.class);
        when(eventFactory.eventBuilder(any())).thenReturn(eventBuilder);
        when(eventBuilder.withEventType(any()).withData(any()).build()).thenReturn(event);
        when(event.toJsonString()).thenReturn(randomString);

        when(recordConverter.convert(any(), any(), any(), any(), any(), any(), anyLong(), anyLong(), any())).thenReturn(event);

        ParquetReader<GenericRecord> parquetReader = mock(ParquetReader.class);
        AvroParquetReader.Builder<GenericRecord> builder = mock(AvroParquetReader.Builder.class);
        BufferAccumulator<Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        doThrow(new RuntimeException("testing")).when(bufferAccumulator).flush();
        when(builder.build()).thenReturn(parquetReader);
        when(parquetReader.read()).thenReturn(mock(GenericRecord.class, RETURNS_DEEP_STUBS), (GenericRecord) null);
        try (MockedStatic<AvroParquetReader> readerMockedStatic = mockStatic(AvroParquetReader.class);
             MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            readerMockedStatic.when(() -> AvroParquetReader.<GenericRecord>builder(any(InputFile.class), any())).thenReturn(builder);
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(any(Buffer.class), anyInt(), any(Duration.class))).thenReturn(bufferAccumulator);

            dataFileLoader.run();
        }

        verify(bufferAccumulator).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(acknowledgementSet).add(event);
        verify(acknowledgementSet, never()).complete();

        verify(exportRecordsTotalCounter).increment();
        verify(bytesReceivedSummary).record(sizeBytes);
        verify(bytesProcessedSummary).record(sizeBytes);
        verify(exportRecordSuccessCounter, never()).increment(1);
        verify(exportRecordErrorCounter).increment(1);
    }

    private DataFileLoader createObjectUnderTest() {
        final InputCodec codec = new ParquetInputCodec(eventFactory);
        return DataFileLoader.create(dataFilePartition, codec, buffer, s3ObjectReader, recordConverter,
                pluginMetrics, sourceCoordinator, acknowledgementSet, acknowledgmentTimeout, dbTableMetadata);
    }
}