/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
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
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.Progress;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.Stats;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.source.S3SelectObjectWorker.CSV_FILE_HEADER_INFO_NONE;
import static org.opensearch.dataprepper.plugins.source.S3SelectObjectWorker.MAX_S3_OBJECT_CHUNK_SIZE;

@ExtendWith(MockitoExtension.class)
class S3SelectObjectWorkerTest {

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private S3AsyncClient s3AsyncClient;

    @Mock
    private S3SelectResponseHandlerFactory responseHandlerFactory;

    @Mock
    private S3SelectResponseHandler selectResponseHandler;

    @Mock
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;

    @Mock
    private Counter s3ObjectsFailedCounter;

    @Mock
    private DistributionSummary s3ObjectEventsSummary;

    @Mock
    private Counter s3ObjectsSucceededCounter;

    @Mock
    private S3ObjectRequest s3ObjectRequest;

    @Mock
    private S3ObjectReference s3ObjectReference;

    @Mock
    private BiConsumer<Event, S3ObjectReference> eventConsumer;

    private ArgumentCaptor<HeadObjectRequest> argumentCaptorForHeadObjectRequest;

    private int numberOfObjectScans;

    @BeforeEach
    void setup() {

        final Random random = new Random();

        numberOfObjectScans = random.nextInt(5) + 1;

        final String bucketName = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();

        given(s3ObjectReference.getBucketName()).willReturn(bucketName);
        given(s3ObjectReference.getKey()).willReturn(objectKey);

        given(s3ObjectRequest.getBuffer()).willReturn(buffer);
        given(s3ObjectRequest.getNumberOfRecordsToAccumulate()).willReturn(1);
        given(s3ObjectRequest.getS3AsyncClient()).willReturn(s3AsyncClient);
        given(s3ObjectRequest.getBufferTimeout()).willReturn(Duration.ofMillis(random.nextInt(100) + 100));
        given(s3ObjectRequest.getS3SelectResponseHandlerFactory()).willReturn(responseHandlerFactory);
        given(responseHandlerFactory.provideS3SelectResponseHandler()).willReturn(selectResponseHandler);
        given(s3ObjectRequest.getEventConsumer()).willReturn(eventConsumer);
        given(s3ObjectRequest.getExpressionType()).willReturn(UUID.randomUUID().toString());

        final S3SelectCSVOption selectCSVOption = mock(S3SelectCSVOption.class);
        given(s3ObjectRequest.getS3SelectCSVOption()).willReturn(selectCSVOption);

        final S3SelectJsonOption selectJsonOption = mock(S3SelectJsonOption.class);
        given(s3ObjectRequest.getS3SelectJsonOption()).willReturn(selectJsonOption);

        given(s3ObjectRequest.getS3ObjectPluginMetrics()).willReturn(s3ObjectPluginMetrics);

        final CompletableFuture<HeadObjectResponse> headObjectResponseCompletableFuture = mock(CompletableFuture.class);
        final HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        given(headObjectResponse.contentLength()).willReturn(MAX_S3_OBJECT_CHUNK_SIZE * numberOfObjectScans);
        given(headObjectResponseCompletableFuture.join()).willReturn(headObjectResponse);


        argumentCaptorForHeadObjectRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
        given(s3AsyncClient.headObject(argumentCaptorForHeadObjectRequest.capture())).willReturn(headObjectResponseCompletableFuture);
    }

    private S3SelectObjectWorker createObjectUnderTest() {
        return new S3SelectObjectWorker(s3ObjectRequest);
    }

    @Test
    void parseS3Object_where_s3SelectResponseHandler_returns_exception_throws_IO_exception() {
        given(s3ObjectRequest.getSerializationFormatOption()).willReturn(S3SelectSerializationFormatOption.CSV);
        given(s3ObjectRequest.getCompressionType()).willReturn(CompressionType.NONE);
        given(selectResponseHandler.getException()).willReturn(mock(Throwable.class));
        given(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).willReturn(s3ObjectsFailedCounter);

        final CompletableFuture<Void> selectObjectResponseFuture = mock(CompletableFuture.class);
        given(selectObjectResponseFuture.join()).willReturn(mock(Void.class));
        given(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class), eq(selectResponseHandler))).willReturn(selectObjectResponseFuture);


        assertThrows(IOException.class, () -> createObjectUnderTest().parseS3Object(s3ObjectReference));

        assertHeadObjectRequestIsCorrect();

        verify(s3ObjectsFailedCounter).increment();

    }

    @Test
    void parseS3Object_with_no_events_in_SelectObjectContentStream_does_not_throw_exception_and_searches_full_scanRange() throws IOException {
        given(s3ObjectRequest.getSerializationFormatOption()).willReturn(S3SelectSerializationFormatOption.CSV);
        given(s3ObjectRequest.getCompressionType()).willReturn(CompressionType.NONE);
        given(selectResponseHandler.getException()).willReturn(null);
        given(selectResponseHandler.getS3SelectContentEvents()).willReturn(Collections.emptyList());

        final CompletableFuture<Void> selectObjectResponseFuture = mock(CompletableFuture.class);
        given(selectObjectResponseFuture.join()).willReturn(mock(Void.class));
        given(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class), eq(selectResponseHandler))).willReturn(selectObjectResponseFuture);

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        assertHeadObjectRequestIsCorrect();

        verify(selectResponseHandler, times(numberOfObjectScans)).getS3SelectContentEvents();
    }

    @ParameterizedTest
    @CsvSource({
            "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}',select * from s3Object,CSV,NONE",
            "'{\"S.No\":\"2\",\"log\":\"data-prep-log\",\"Date\":\"2023-03-03\"}',select * from s3Object,JSON,NONE",
            "'{\"S.No\":\"3\",\"name\":\"data-prep-test\",\"age\":\"21y\"}',select * from s3Object,PARQUET,NONE",
            "'{\"S.No\":\"4\",\"name\":\"data-prep\",\"empId\",\"123456\"}',select * from s3Object,CSV,GZIP",
            "'{\"S.No\":\"5\",\"log\":\"data-prep-log\",\"documentType\":\"test doc\"}',select * from s3Object,JSON,GZIP",
            "'{\"S.No\":\"6\",\"name\":\"data-prep-test\",\"type\":\"json\"}',select * from s3Object,PARQUET,GZIP"})
    void parseS3Object_with_different_formats_and_events_in_the_inputstream_works_as_expected(final String responseFormat,
                                                                                              final String query,
                                                                                              final String format,
                                                                                              final String compression) throws IOException {
        if (format.equals("CSV")) {
            given(s3ObjectRequest.getS3SelectCSVOption().getFileHeaderInfo()).willReturn(CSV_FILE_HEADER_INFO_NONE);
            given(s3ObjectRequest.getS3SelectCSVOption().getComments()).willReturn(UUID.randomUUID().toString());
            given(s3ObjectRequest.getS3SelectCSVOption().getQuiteEscape()).willReturn(UUID.randomUUID().toString());
        } else if (format.equals("JSON")) {
            given(s3ObjectRequest.getS3SelectJsonOption().getType()).willReturn(UUID.randomUUID().toString());
        }

        given(s3ObjectRequest.getCompressionType()).willReturn(CompressionType.valueOf(compression));
        given(s3ObjectRequest.getExpression()).willReturn(query);
        given(s3ObjectRequest.getSerializationFormatOption()).willReturn(S3SelectSerializationFormatOption.valueOf(format));
        given(selectResponseHandler.getException()).willReturn(null);
        given(selectResponseHandler.getS3SelectContentEvents()).willReturn(constructObjectEventStreamForResponseFormat(responseFormat));
        given(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).willReturn(s3ObjectEventsSummary);
        given(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).willReturn(s3ObjectsSucceededCounter);

        final CompletableFuture<Void> selectObjectResponseFuture = mock(CompletableFuture.class);
        given(selectObjectResponseFuture.join()).willReturn(mock(Void.class));
        given(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class), eq(selectResponseHandler))).willReturn(selectObjectResponseFuture);
        doAnswer(invocation -> null).when(eventConsumer).accept(any(Event.class), eq(s3ObjectReference));

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        assertHeadObjectRequestIsCorrect();

        verify(eventConsumer).accept(any(Event.class), eq(s3ObjectReference));
        verify(s3ObjectEventsSummary).record(1);
        verify(s3ObjectsSucceededCounter).increment();
    }

    @ParameterizedTest
    @CsvSource({
            "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}',select * from s3Object,CSV,NONE"})
    void parseS3Object_retries_bufferAccumulatorFlush_when_bufferWriteAll_throwsTimeoutException(final String responseFormat,
                                                                                                 final String query,
                                                                                                 final String format,
                                                                                                 final String compression) throws Exception {
        given(s3ObjectRequest.getCompressionType()).willReturn(CompressionType.valueOf(compression));
        given(s3ObjectRequest.getExpression()).willReturn(query);
        given(s3ObjectRequest.getSerializationFormatOption()).willReturn(S3SelectSerializationFormatOption.valueOf(format));
        given(selectResponseHandler.getException()).willReturn(null);
        given(selectResponseHandler.getS3SelectContentEvents()).willReturn(constructObjectEventStreamForResponseFormat(responseFormat));
        given(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).willReturn(s3ObjectEventsSummary);
        given(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).willReturn(s3ObjectsSucceededCounter);
        given(s3ObjectRequest.getS3SelectCSVOption().getFileHeaderInfo()).willReturn(CSV_FILE_HEADER_INFO_NONE);
        given(s3ObjectRequest.getS3SelectCSVOption().getComments()).willReturn(UUID.randomUUID().toString());
        given(s3ObjectRequest.getS3SelectCSVOption().getQuiteEscape()).willReturn(UUID.randomUUID().toString());

        final CompletableFuture<Void> selectObjectResponseFuture = mock(CompletableFuture.class);
        given(selectObjectResponseFuture.join()).willReturn(mock(Void.class));
        given(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class), eq(selectResponseHandler))).willReturn(selectObjectResponseFuture);
        doAnswer(invocation -> null).when(eventConsumer).accept(any(Event.class), eq(s3ObjectReference));

        doThrow(TimeoutException.class).doNothing().when(buffer).writeAll(any(Collection.class), anyInt());

        createObjectUnderTest().parseS3Object(s3ObjectReference);

        assertHeadObjectRequestIsCorrect();

        verify(eventConsumer).accept(any(Event.class), eq(s3ObjectReference));
        verify(s3ObjectEventsSummary).record(1);
        verify(s3ObjectsSucceededCounter).increment();
    }

    private void assertHeadObjectRequestIsCorrect() {
        final HeadObjectRequest capturedRequest = argumentCaptorForHeadObjectRequest.getValue();

        assertThat(capturedRequest.key(), equalTo(s3ObjectReference.getKey()));
        assertThat(capturedRequest.bucket(), equalTo(s3ObjectReference.getBucketName()));
    }

    private List<SelectObjectContentEventStream> constructObjectEventStreamForResponseFormat(final String responseFormat) {
        return new ArrayList<>(Arrays.asList(
                SelectObjectContentEventStream.recordsBuilder().payload(SdkBytes.fromUtf8String(responseFormat)).build(),
                SelectObjectContentEventStream.contBuilder().build(),
                SelectObjectContentEventStream.statsBuilder()
                        .details(Stats.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.progressBuilder()
                        .details(Progress.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.endBuilder().build()));
    }
}