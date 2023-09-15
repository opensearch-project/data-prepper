/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ContinuationEvent;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.Progress;
import software.amazon.awssdk.services.s3.model.ProgressEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.Stats;
import software.amazon.awssdk.services.s3.model.StatsEvent;
import software.amazon.eventstream.HeaderValue;
import software.amazon.eventstream.Message;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class S3SelectResponseHandlerTest {
    private S3AsyncClient s3;
    private static final Region TEST_REGION = Region.US_WEST_2;
    private static final AwsCredentialsProvider TEST_CREDENTIALS = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("testAID", "testSKID"));
    @Rule
    public WireMockRule wireMock = new WireMockRule(0);
    @Before
    public void setup() {
        s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:" + wireMock.port()))
                .serviceConfiguration(c -> c.pathStyleAccessEnabled(true))
                .region(TEST_REGION)
                .credentialsProvider(TEST_CREDENTIALS)
                .build();
    }

    @Test
    public void testS3SelectResponseHandlerWithRuntimeException() throws IOException {
        S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final RuntimeException exception = mock(RuntimeException.class);
        responseHandler.exceptionOccurred(exception);
        assertThat(responseHandler.getException(),equalTo(exception));
    }
    @Test
    public void testS3SelectResponseHandlerWithMultipleEvents() throws IOException {
        S3SelectResponseHandler testHandler = new S3SelectResponseHandler();
        String s3Response = "{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}\n" +
                "{\"S.No\":\"2\",\"name\":\"test2\",\"country\":\"IND\"}\n" +
                "{\"S.No\":\"3\",\"name\":\"test3\",\"country\":\"CANADA\"}";
        List<SelectObjectContentEventStream> events = Arrays.asList(
                SelectObjectContentEventStream.recordsBuilder().payload(SdkBytes.fromUtf8String(s3Response)).build(),
                SelectObjectContentEventStream.contBuilder().build(),
                SelectObjectContentEventStream.statsBuilder().details(Stats.builder()
                        .bytesProcessed(1L)
                        .bytesScanned(2L)
                        .bytesReturned(3L)
                        .build()).build(),
                SelectObjectContentEventStream.progressBuilder().details(Progress.builder()
                        .bytesProcessed(1L)
                        .bytesScanned(2L)
                        .bytesReturned(3L)
                        .build()).build(),
                SelectObjectContentEventStream.endBuilder().build());

        stubFor(any(anyUrl()).willReturn(aResponse().withStatus(200).withBody(encodedEvents(events))));

        runSimpleQuery(s3, testHandler).join();
        InputStream inputStream = getInputStreamFromResponseHeader(testHandler);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        assertThat(testHandler.getException(), nullValue());
        assertThat(testHandler.getS3SelectContentEvents(), equalTo(events));
        assertThat(reader.readLine(),equalTo("{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}"));
        assertThat(reader.readLine(),equalTo("{\"S.No\":\"2\",\"name\":\"test2\",\"country\":\"IND\"}"));
        assertThat(reader.readLine(),equalTo("{\"S.No\":\"3\",\"name\":\"test3\",\"country\":\"CANADA\"}"));
    }

    private InputStream getInputStreamFromResponseHeader(final S3SelectResponseHandler responseHand) {
        return responseHand.getS3SelectContentEvents().stream()
                .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
                .map(e -> ((RecordsEvent) e).payload().asInputStream()).collect(Collectors.toList()).stream()
                .reduce(SequenceInputStream::new).orElse(null);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodedEvents(List<SelectObjectContentEventStream> events) {
        ByteArrayOutputStream eventStreamBytes = new ByteArrayOutputStream();
        MarshallingVisitor marshaller = new MarshallingVisitor();
        events.stream()
                .map(e -> {
                    marshaller.reset();
                    e.accept(marshaller);
                    Map<String, HeaderValue> headers = new HashMap<>();
                    headers.put(":message-type", HeaderValue.fromString("event"));
                    headers.put(":event-type", HeaderValue.fromString(e.sdkEventType().toString()));
                    return new Message(headers, marshaller.marshalledBytes());
                })
                .forEach(m -> m.encode(eventStreamBytes));
        return eventStreamBytes.toByteArray();
    }
    @After
    public void teardown() {
        s3.close();
    }
    private static CompletableFuture<Void> runSimpleQuery(S3AsyncClient s3, SelectObjectContentResponseHandler handler) {
        InputSerialization inputSerialization = InputSerialization.builder()
                .json(JSONInput.builder().build())
                .compressionType(CompressionType.NONE)
                .build();
        OutputSerialization outputSerialization = OutputSerialization.builder()
                .json(JSONOutput.builder().build())
                .build();
        SelectObjectContentRequest select = SelectObjectContentRequest.builder()
                .bucket("test-bucket")
                .key("test-key")
                .expression("select * from s3object")
                .expressionType(ExpressionType.SQL)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization)
                .build();
        return s3.selectObjectContent(select, handler);
    }

    private static class MarshallingVisitor implements SelectObjectContentResponseHandler.Visitor {
        private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        @Override
        public void visitEnd(EndEvent event) {
            // no payload
        }
        @Override
        public void visitCont(ContinuationEvent event) {
            // no payload
        }
        @Override
        public void visitRecords(RecordsEvent event) {
            writeUnchecked(event.payload().asByteArray());
        }
        @Override
        public void visitStats(StatsEvent event) {
            Stats details = event.details();
            writeUnchecked(bytes("<Details>"));
            writeUnchecked(bytes("<BytesScanned>"));
            writeUnchecked(bytes(details.bytesScanned().toString()));
            writeUnchecked(bytes("</BytesScanned>"));
            writeUnchecked(bytes("<BytesProcessed>"));
            writeUnchecked(bytes(details.bytesProcessed().toString()));
            writeUnchecked(bytes("</BytesProcessed>"));
            writeUnchecked(bytes("<BytesReturned>"));
            writeUnchecked(bytes(details.bytesReturned().toString()));
            writeUnchecked(bytes("</BytesReturned>"));
            writeUnchecked(bytes("</Details>"));
        }

        @Override
        public void visitProgress(ProgressEvent event) {
            Progress details = event.details();
            writeUnchecked(bytes("<Details>"));
            writeUnchecked(bytes("<BytesScanned>"));
            writeUnchecked(bytes(details.bytesScanned().toString()));
            writeUnchecked(bytes("</BytesScanned>"));
            writeUnchecked(bytes("<BytesProcessed>"));
            writeUnchecked(bytes(details.bytesProcessed().toString()));
            writeUnchecked(bytes("</BytesProcessed>"));
            writeUnchecked(bytes("<BytesReturned>"));
            writeUnchecked(bytes(details.bytesReturned().toString()));
            writeUnchecked(bytes("</BytesReturned>"));
            writeUnchecked(bytes("</Details>"));
        }

        public byte[] marshalledBytes() {
            return byteArrayOutputStream.toByteArray();
        }

        public void reset() {
            byteArrayOutputStream.reset();
        }

        private void writeUnchecked(byte[] data) {
            try {
                byteArrayOutputStream.write(data);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
