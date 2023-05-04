/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3ScanObjectWorkerTest {
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private Duration bufferTimeout;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock
    private S3Client s3Client;
    @Mock
    private Counter s3ObjectsFailedCounter;
    @Mock
    private Counter s3ObjectsFailedNotFoundCounter;
    @Mock
    private Counter s3ObjectsFailedAccessDeniedCounter;
    @Mock
    private Counter s3ObjectsSucceededCounter;
    @Mock
    private Timer s3ObjectReadTimer;
    @Mock
    private DistributionSummary s3ObjectSizeSummary;
    @Mock
    private DistributionSummary s3ObjectSizeProcessedSummary;
    @Mock
    private DistributionSummary s3ObjectEventsSummary;
    @Mock
    private ResponseInputStream<GetObjectResponse> objectInputStream;
    @Mock
    private GetObjectResponse getObjectResponse;
    private long objectSize;
    private int recordsToAccumulate;
    @Mock
    private CompressionEngine compressionEngine;

    @Mock
    private ListObjectsV2Response listObjectsV2Response;

    @Mock
    private S3ObjectRequest s3ObjectRequest;

    @Mock
    private S3SelectResponseHandlerFactory responseHandlerFactory;

    @Mock
    private S3SelectResponseHandler selectResponseHandler;

    @Mock
    private S3AsyncClient s3AsyncClient;

    @Mock
    private S3ObjectHandler s3ObjectHandler;

    ScanObjectWorker createScanWorker(final ScanOptions scanOptions,
                                      final String scanObjectName,
                                      final S3ObjectHandler s3ObjectHandlerForCheck) throws IOException {
        Random random = new Random();
        recordsToAccumulate = random.nextInt(10) + 2;
        objectSize = random.nextInt(100_000) + 10_000;
        objectInputStream = mock(ResponseInputStream.class);
        getObjectResponse = mock(GetObjectResponse.class);
        s3Client = mock(S3Client.class);
        lenient().when(objectInputStream.response()).thenReturn(getObjectResponse);
        lenient().when(getObjectResponse.contentLength()).thenReturn(objectSize);
        lenient().when(getObjectResponse.lastModified()).thenReturn(LocalDateTime.of(2023, 03, 06, 10, 10).atZone(ZoneId.systemDefault()).toInstant());
        listObjectsV2Response = mock(ListObjectsV2Response.class);
        S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn(scanObjectName);
        when(listObjectsV2Response.contents()).thenReturn(Arrays.asList(s3Object));
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        s3ObjectsFailedCounter = mock(Counter.class);
        s3ObjectsSucceededCounter = mock(Counter.class);
        s3ObjectReadTimer = mock(Timer.class);
        compressionEngine = mock(CompressionEngine.class);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        s3ObjectRequest = mock(S3ObjectRequest.class);
        S3ObjectPluginMetrics s3PluginMetrics = mock(S3ObjectPluginMetrics.class);
        when(s3PluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);
        when(s3PluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(s3ObjectsFailedNotFoundCounter);
        when(s3PluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(s3ObjectsFailedAccessDeniedCounter);
        when(s3PluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3PluginMetrics.getS3ObjectReadTimer()).thenReturn(s3ObjectReadTimer);
        when(s3PluginMetrics.getS3ObjectSizeSummary()).thenReturn(s3ObjectSizeSummary);
        when(s3PluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(s3ObjectSizeProcessedSummary);
        when(s3PluginMetrics.getS3ObjectEventsSummary()).thenReturn(s3ObjectEventsSummary);
        when(s3PluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        when(s3ObjectRequest.getS3ObjectPluginMetrics()).thenReturn(s3PluginMetrics);
        when(bucketOwnerProvider.getBucketOwner("my-bucket-1")).thenReturn(Optional.of("my-bucket-1"));
        when(s3ObjectRequest.getBucketOwnerProvider()).thenReturn(bucketOwnerProvider);
        lenient().when(compressionEngine.createInputStream("file1.csv", objectInputStream)).thenReturn(objectInputStream);
        if (s3ObjectHandlerForCheck instanceof S3ObjectWorker)
            s3ObjectHandler = new S3ObjectWorker(s3ObjectRequest);
        else if (s3ObjectHandlerForCheck instanceof S3SelectObjectWorker) {
            selectResponseHandler = mock(S3SelectResponseHandler.class);
            s3AsyncClient = mock(S3AsyncClient.class);
            S3SelectCSVOption s3SelectCSVOption = mock(S3SelectCSVOption.class);
            responseHandlerFactory = mock(S3SelectResponseHandlerFactory.class);
            given(s3ObjectRequest.getS3AsyncClient()).willReturn(s3AsyncClient);
            when(s3SelectCSVOption.getFileHeaderInfo()).thenReturn("csv");
            when(s3ObjectRequest.getS3SelectCSVOption()).thenReturn(s3SelectCSVOption);
            when(s3ObjectRequest.getSerializationFormatOption()).thenReturn(S3SelectSerializationFormatOption.CSV);
            given(s3ObjectRequest.getS3SelectResponseHandlerFactory()).willReturn(responseHandlerFactory);
            given(responseHandlerFactory.provideS3SelectResponseHandler()).willReturn(selectResponseHandler);
            final CompletableFuture<Void> selectObjectResponseFuture = mock(CompletableFuture.class);
            given(selectObjectResponseFuture.join()).willReturn(mock(Void.class));
            given(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),
                    eq(selectResponseHandler))).willReturn(selectObjectResponseFuture);
            s3ObjectHandler = new S3SelectObjectWorker(s3ObjectRequest);
        }

        return new ScanObjectWorker(s3Client, Arrays.asList(scanOptions), s3ObjectHandler,bucketOwnerProvider);
    }

    @Test
    void s3_scan_bucket_with_s3Object_verify_start_time_and_range_combination() throws IOException {
        final String scanObjectName = "sample.csv";
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(List.of(scanObjectName));
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setStartDateTime(LocalDateTime.parse("2023-03-06T00:00:00"))
                .setRange(Duration.parse("P2DT1H"))
                .setBucket("my-bucket-1")
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, scanObjectName, mock(S3ObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        verify(s3ObjectsSucceededCounter).increment();
        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo("my-bucket-1"));
        assertThat(actualGetObjectRequest.key(), equalTo(scanObjectName));

    }

    @Test
    void s3_scan_bucket_with_s3Object_verify_start_time_and_end_time_combination() throws IOException {
        final String scanObjectName = "sample1.csv";
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(List.of(scanObjectName));
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setStartDateTime(LocalDateTime.parse("2023-03-06T00:00:00"))
                .setEndDateTime(LocalDateTime.parse("2023-04-09T00:00:00"))
                .setBucket("my-bucket-1")
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, scanObjectName, mock(S3ObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        verify(s3ObjectsSucceededCounter).increment();
        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo("my-bucket-1"));
        assertThat(actualGetObjectRequest.key(), equalTo(scanObjectName));
    }

    @Test
    void s3_scan_bucket_with_s3Object_verify_end_time_and_range_combination() throws IOException {
        final String scanObjectName = "test.csv";
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(List.of(scanObjectName));
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setEndDateTime(LocalDateTime.parse("2023-03-09T00:00:00"))
                .setRange(Duration.parse("P10DT2H"))
                .setBucket("my-bucket-1")
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, scanObjectName, mock(S3ObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        verify(s3ObjectsSucceededCounter).increment();
        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo("my-bucket-1"));
        assertThat(actualGetObjectRequest.key(), equalTo(scanObjectName));
    }

    @Test
    void s3_scan_bucket_with_s3Object_skip_processed_key() throws IOException {
        final String scanObjectName = "test.csv";
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(List.of(scanObjectName));
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setEndDateTime(LocalDateTime.parse("2023-03-09T00:00:00"))
                .setRange(Duration.parse("P10DT2H"))
                .setBucket("my-bucket-1")
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, scanObjectName, mock(S3ObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        verify(s3ObjectsSucceededCounter, times(0)).increment();
        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo("my-bucket-1"));
        assertThat(actualGetObjectRequest.key(), equalTo(scanObjectName));
    }

    @Test
    void s3_scan_bucket_with_s3_select_verify_end_time_and_range_combination() throws IOException {
        final String startDateTime = "2023-03-07T10:00:00";
        final String bucketName = "my-bucket-1";
        final List<String> keyPathList = Arrays.asList("file3.csv");
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(keyPathList);
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setEndDateTime(LocalDateTime.parse(startDateTime))
                .setRange(Duration.parse("P10DT2H"))
                .setBucket(bucketName)
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, keyPathList.get(0), mock(S3SelectObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        verify(s3ObjectsSucceededCounter).increment();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(keyPathList.get(0)));

    }

    @Test
    void s3_scan_bucket_with_s3_select_verify_start_time_and_range_combination() throws IOException {
        final String bucketName = "my-bucket-1";
        final List<String> keyPathList = Arrays.asList("file2.csv");
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(keyPathList);
        final S3SelectSerializationFormatOption s3SelectSerializationFormatOption =
                S3SelectSerializationFormatOption.valueOf("CSV");
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setStartDateTime(LocalDateTime.parse("2023-03-05T10:00:00"))
                .setRange(Duration.parse("P10DT2H"))
                .setBucket(bucketName)
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, keyPathList.get(0), mock(S3SelectObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        verify(s3ObjectsSucceededCounter).increment();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(keyPathList.get(0)));
    }

    @Test
    void s3_scan_bucket_with_s3_select_verify_start_time_and_end_time_combination() throws IOException {
        final String bucketName = "my-bucket-1";
        final List<String> keyPathList = Arrays.asList("file1.csv");
        S3ScanKeyPathOption s3ScanKeyPathOption = mock(S3ScanKeyPathOption.class);
        when(s3ScanKeyPathOption.getS3scanIncludeOptions()).thenReturn(keyPathList);
        final ScanOptions scanOptions = new ScanOptions.Builder()
                .setStartDateTime(LocalDateTime.parse("2023-03-05T10:00:00"))
                .setEndDateTime(LocalDateTime.parse("2023-04-09T00:00:00"))
                .setBucket(bucketName)
                .setS3ScanKeyPathOption(s3ScanKeyPathOption).build();
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptions, keyPathList.get(0), mock(S3SelectObjectWorker.class));
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        verify(s3ObjectsSucceededCounter).increment();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo(keyPathList.get(0)));
    }

    @Test
    void s3_scan_service_whole_bucket_scan_test() throws IOException {
            final String scanObjectName = "bucket-test.csv";
            final ScanOptions scanOptions = new ScanOptions.Builder()
                    .setEndDateTime(LocalDateTime.parse("2023-03-09T00:00:00"))
                    .setRange(Duration.parse("P10DT2H"))
                    .setBucket("my-bucket-1").build();
            final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                        .thenReturn(bufferAccumulator);
                final ScanObjectWorker scanWorker = createScanWorker(scanOptions, scanObjectName, mock(S3ObjectWorker.class));
                scanWorker.run();
            }
            final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
            verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

            verify(s3ObjectsSucceededCounter).increment();
            final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
            assertThat(actualGetObjectRequest, notNullValue());
            assertThat(actualGetObjectRequest.bucket(), equalTo("my-bucket-1"));
            assertThat(actualGetObjectRequest.key(), equalTo(scanObjectName));
        }
}
