/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.Progress;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.Stats;

@ExtendWith(MockitoExtension.class)
class S3SelectObjectWorkerTest {
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private S3AsyncClient s3AsyncClient;
    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private DistributionSummary distributionSummary;
    @Mock
    S3ObjectReference s3ObjectReference;
    @Mock
    private S3SelectOptions s3SelectOptions;
    private String key;
    private String bucketName;
    @Mock
    private Duration bufferTimeout;
    @Mock
    private Counter s3ObjectsFailedCounter;
    @Mock
    private Counter s3ObjectsSucceededCounter;
    @Mock
    private S3SelectResponseHandler selectResponseHandler;
    @Mock
    private List<SelectObjectContentEventStream> selectObjectContentEventStreamList;

    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    @Mock
    private BiConsumer<Event, S3ObjectReference> eventConsumer;

    public void selectObjectContentResponse(final String responseFormat){
        selectObjectContentEventStreamList = new ArrayList<>();
        CompletableFuture<Void> feature = mock(CompletableFuture.class);
        selectObjectContentEventStreamList.addAll(Arrays.asList(
                SelectObjectContentEventStream.recordsBuilder().payload(SdkBytes.fromUtf8String(responseFormat)).build(),
                SelectObjectContentEventStream.contBuilder().build(),
                SelectObjectContentEventStream.statsBuilder()
                        .details(Stats.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.progressBuilder()
                        .details(Progress.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.endBuilder().build()));
        lenient().when(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),any(SelectObjectContentResponseHandler.class))).thenReturn(feature);
        lenient().when(selectResponseHandler.getS3SelectContentEvents()).thenReturn(selectObjectContentEventStreamList);
    }

    private S3SelectObjectWorker createSelectObjectUnderTest(final String responseFormat, final String expression,
                                            final S3SelectSerializationFormatOption format,
                                            final S3SelectResponseHandler selectResponseHandlerTest,
                                            final S3ObjectPluginMetrics s3ObjectPluginMetrics,
                                            final CompressionType compressionType) throws Exception {
        if(selectResponseHandlerTest == null)
            selectObjectContentResponse(responseFormat);
        Random random = new Random();
        int numberOfRecordsToAccumulate = random.nextInt(10) + 2;
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);
        when(s3SourceConfig.getS3SelectOptions()).thenReturn(s3SelectOptions);
        when(s3SourceConfig.getS3SelectOptions().getExpression()).thenReturn(expression);
        final S3SelectCSVOption csvOption = new S3SelectCSVOption();
        ReflectivelySetField.setField(S3SelectCSVOption.class,csvOption,"fileHeaderInfo","none");
        S3ObjectRequest request = new S3ObjectRequest.Builder(buffer,numberOfRecordsToAccumulate,
                bufferTimeout,s3ObjectPluginMetrics)
                .expression(expression).eventConsumer(eventConsumer)
                .serializationFormatOption(format).s3SelectCSVOption(csvOption)
                .s3SelectJsonOption(new S3SelectJsonOption())
                .s3AsyncClient(s3AsyncClient).compressionType(compressionType)
                .s3SelectResponseHandler(selectResponseHandler).build();
        return new S3SelectObjectWorker(request);
    }
    @Test
    void selectObjectFromS3CsvTestWithEmptyResponse() throws Exception{
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(s3ObjectsFailedCounter);
        S3SelectResponseHandler selectResponseHandler =new S3SelectResponseHandler();
        final S3SelectObjectWorker selectObjectUnderTest = createSelectObjectUnderTest(null, "select * from s3Object", S3SelectSerializationFormatOption.CSV,
                selectResponseHandler, s3ObjectPluginMetrics, CompressionType.NONE);
        final ArgumentCaptor<SelectObjectContentRequest> request = ArgumentCaptor
                .forClass(SelectObjectContentRequest.class);
        final IOException exception = assertThrows(IOException.class, () -> selectObjectUnderTest.parseS3Object(s3ObjectReference));
        assertNotNull(exception);
        verify(s3AsyncClient).selectObjectContent(request.capture(), any(S3SelectResponseHandler.class));
        assertThat(request.getValue().key(), equalTo(key));
        assertThat(request.getValue().bucket(), equalTo(bucketName));
        assertThat(request.getValue().expectedBucketOwner(), nullValue());
        assertThat(request.getValue().expression(), equalTo(s3SourceConfig.getS3SelectOptions().getExpression()));
        verify(s3ObjectsFailedCounter).increment();
        assertTrue(selectResponseHandler.getS3SelectContentEvents().isEmpty());
    }
    @ParameterizedTest
    @CsvSource({
            "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}',select * from s3Object,CSV,NONE",
            "'{\"S.No\":\"2\",\"log\":\"data-prep-log\",\"Date\":\"2023-03-03\"}',select * from s3Object,JSON,NONE",
            "'{\"S.No\":\"3\",\"name\":\"data-prep-test\",\"age\":\"21y\"}',select * from s3Object,PARQUET,NONE",
            "'{\"S.No\":\"4\",\"name\":\"data-prep\",\"empId\",\"123456\"}',select * from s3Object,CSV,GZIP",
            "'{\"S.No\":\"5\",\"log\":\"data-prep-log\",\"documentType\":\"test doc\"}',select * from s3Object,JSON,GZIP",
            "'{\"S.No\":\"6\",\"name\":\"data-prep-test\",\"type\":\"json\"}',select * from s3Object,PARQUET,GZIP"})
    void selectObjectFromS3TestWithCorrectRequest(final String responseFormat,final String query,final String format,final String compression) throws Exception {
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(s3ObjectsSucceededCounter);
        final S3SelectObjectWorker selectObjectUnderTest = createSelectObjectUnderTest(responseFormat, query, S3SelectSerializationFormatOption.valueOf(format),
                null, s3ObjectPluginMetrics, CompressionType.valueOf(compression));
        final ArgumentCaptor<SelectObjectContentRequest> request = ArgumentCaptor.forClass(SelectObjectContentRequest.class);
        selectObjectUnderTest.parseS3Object(s3ObjectReference);
        verify(s3AsyncClient).selectObjectContent(request.capture(), any(S3SelectResponseHandler.class));
        assertThat(request.getValue().key(), equalTo(key));
        assertThat(request.getValue().bucket(), equalTo(bucketName));
        assertThat(request.getValue().expectedBucketOwner(), nullValue());
        assertThat(request.getValue().expression(), equalTo(s3SourceConfig.getS3SelectOptions().getExpression()));
        assertTrue(selectResponseHandler.getS3SelectContentEvents().isEmpty());
        verify(s3ObjectsSucceededCounter).increment();
        assertThat(distributionSummary,notNullValue());
    }
}