/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3ScanObjectWorkerTest {
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private int numberOfRecordsToAccumulate;
    @Mock
    private Duration bufferTimeout;
    @Mock
    private BiConsumer<Event, S3ObjectReference> eventMetadataModifier;
    @Mock
    private BucketOwnerProvider bucketOwnerProvider;
    @Mock
    private S3Client s3Client;
    @Mock
    private S3AsyncClient s3AsyncClient;
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
    private Codec codec;
    @Mock
    private CompressionEngine compressionEngine;

    ScanObjectWorker createScanWorker(final ScanOptionsBuilder scanOptionsBuilder) throws IOException {
        Random random = new Random();
        recordsToAccumulate = random.nextInt(10) + 2;
        objectSize = random.nextInt(100_000) + 10_000;
        objectInputStream = mock(ResponseInputStream.class);
        getObjectResponse = mock(GetObjectResponse.class);
        lenient().when(objectInputStream.response()).thenReturn(getObjectResponse);
        lenient().when(getObjectResponse.contentLength()).thenReturn(objectSize);
        lenient().when(getObjectResponse.lastModified()).thenReturn(LocalDateTime.of(2023,03,06,10,10).atZone(ZoneId.systemDefault()).toInstant());
        s3Client = mock(S3Client.class);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(objectInputStream);
        s3ObjectsFailedCounter = mock(Counter.class);
        s3ObjectsSucceededCounter = mock(Counter.class);
        s3ObjectReadTimer = mock(Timer.class);
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

        compressionEngine = mock(CompressionEngine.class);
        lenient().when(compressionEngine.createInputStream("file1.csv", objectInputStream)).thenReturn(objectInputStream);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer,numberOfRecordsToAccumulate,bufferTimeout,s3PluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3AsyncClient(s3AsyncClient).s3Client(s3Client)
                .eventConsumer(eventMetadataModifier).build();
        return new ScanObjectWorker(s3ObjectRequest,Arrays.asList(scanOptionsBuilder));
    }
    @ParameterizedTest
    @CsvSource({"1w","2d","3m","1y"})
    void fileScanBucketWithS3ObjectVerifyingRangeInYears(final String range) throws IOException{
        final String startDateTime="2023-03-07T10:00:00";
        final String bucketName = "my-bucket-2";
        final List<String> keyPathList = Arrays.asList("sample.csv");
        final ScanOptionsBuilder scanOptionsBuilder = new ScanOptionsBuilder().setStartDate(startDateTime).setRange(range).setBucket(bucketName).setQuery(null).setSerializationFormatOption(null).setKeys(keyPathList).setCodec(codec).setCompressionOption(CompressionOption.NONE);
        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptionsBuilder);
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo("sample.csv"));
    }
    @ParameterizedTest
    @CsvSource({
            "2d,select count(*) from s3Object s,CSV",
            "5d,select count(*) from s3Object s,JSON",
            "20d,select count(*) from s3Object s,PARQUET",
            "1w,select s._1 from s3Object s,CSV",
            "2w,select s._4 from s3Object s,JSON",
            "3w,select s._3 from s3Object s,CSV",
            "1m,select s._9 from s3Object s,JSON",
            "2m,select s._9 from s3Object s,PARQUET",
            "3m,select * from s3Object s,PARQUET",
            "1y,select *._9 from s3Object s,CSV",
            "5y,select * from s3Object s,JSON",
            "3y,select s.* from s3Object s,PARQUET"
    })
    void scanBucketWithS3SelectVerifyingRangeInDays(String range,final String queryStatement,final String dataSerializingFormat) throws IOException {
        final String startDateTime="2023-03-07T10:00:00";
        final String bucketName = "my-bucket-1";
        final List<String> keyPathList = Arrays.asList("file1.csv");
        final S3SelectSerializationFormatOption s3SelectSerializationFormatOption = S3SelectSerializationFormatOption.valueOf(dataSerializingFormat);
        final ScanOptionsBuilder scanOptionsBuilder = new ScanOptionsBuilder().setStartDate(startDateTime).setRange(range).setBucket(bucketName).setQuery(queryStatement).setSerializationFormatOption(s3SelectSerializationFormatOption).setKeys(keyPathList).setCodec(null).setCompressionOption(CompressionOption.NONE);

        final BufferAccumulator bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout))
                    .thenReturn(bufferAccumulator);
            final ScanObjectWorker scanWorker = createScanWorker(scanOptionsBuilder);
            scanWorker.run();
        }
        final ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        final GetObjectRequest actualGetObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(actualGetObjectRequest, notNullValue());
        assertThat(actualGetObjectRequest.bucket(), equalTo(bucketName));
        assertThat(actualGetObjectRequest.key(), equalTo("file1.csv"));
    }
}
