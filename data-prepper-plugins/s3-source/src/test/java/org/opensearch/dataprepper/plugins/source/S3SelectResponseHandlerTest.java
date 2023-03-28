/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.Progress;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.Stats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

public class S3SelectResponseHandlerTest {
    @Test
    public void testS3SelectResponseHandlerWithMockedResponse() throws IOException {
        S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final SdkPublisher publisher = mock(SdkPublisher.class);
        responseHandler.onEventStream(publisher);
        responseHandler.responseReceived(mock(SelectObjectContentResponse.class));
        responseHandler.complete();
        final List<SelectObjectContentEventStream> receivedEvents =
                responseHandler.getS3SelectContentEvents();
        assertNotNull(receivedEvents);
    }

    @Test
    public void testS3SelectResponseHandler() throws IOException {
        S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final RuntimeException exception = mock(RuntimeException.class);
        responseHandler.exceptionOccurred(exception);
        responseHandler.complete();
        final List<SelectObjectContentEventStream> receivedEvents =
                responseHandler.getS3SelectContentEvents();
        assertNotNull(receivedEvents);
    }

    @ParameterizedTest
    @ValueSource(strings= {"{\"name\":\"Test\"}",
            "{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}",
            "{\"S.No\":\"6\",\"name\":\"data-prep-test\",\"type\":\"json\"}",
            "{\"S.No\":\"5\",\"log\":\"data-prep-log\",\"documentType\":\"test doc\"}",
            "{\"S.No\":\"4\",\"name\":\"data-prep\",\"empId\",\"123456\"}"})
    public void testS3SelectResponseHandlerWithMultipleEvents(final String response) throws IOException {
        S3SelectResponseHandler responseHandler = mock(S3SelectResponseHandler.class);
        List<SelectObjectContentEventStream> selectObjectContentEventStreamList = Arrays.asList(
                SelectObjectContentEventStream.recordsBuilder().payload(SdkBytes.fromUtf8String(response)).build(),
                SelectObjectContentEventStream.contBuilder().build(),
                SelectObjectContentEventStream.statsBuilder()
                        .details(Stats.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.progressBuilder()
                        .details(Progress.builder().bytesProcessed(10L).bytesScanned(20L).bytesReturned(30L).build()).build(),
                SelectObjectContentEventStream.endBuilder().build());
        lenient().when(responseHandler.getS3SelectContentEvents()).thenReturn(selectObjectContentEventStreamList);
        final RuntimeException exception = mock(RuntimeException.class);
        responseHandler.exceptionOccurred(exception);
        responseHandler.complete();
        List<SelectObjectContentEventStream> receivedEvents = responseHandler.getS3SelectContentEvents();
        final InputStream stream = getInputStreamFromResponseHeader(responseHandler);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        assertThat(receivedEvents,notNullValue());
        assertThat(receivedEvents,sameInstance(selectObjectContentEventStreamList));
        assertThat(reader.readLine(),equalTo(response));

    }

    private InputStream getInputStreamFromResponseHeader(final S3SelectResponseHandler responseHand) {
        return responseHand.getS3SelectContentEvents().stream()
                .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
                .map(e -> ((RecordsEvent) e).payload().asInputStream()).collect(Collectors.toList()).stream()
                .reduce(SequenceInputStream::new).orElse(null);
    }
}
