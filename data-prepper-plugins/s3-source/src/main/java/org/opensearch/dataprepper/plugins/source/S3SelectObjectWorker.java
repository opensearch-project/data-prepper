/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.input.CountingInputStream;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.ScanRange;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Class responsible for taking an {@link S3SelectResponseHandler} and creating
 * all the necessary {@link Event} objects in the Data Prepper {@link Buffer}.
 */
public class S3SelectObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(S3SelectObjectWorker.class);

    static final long MAX_S3_OBJECT_CHUNK_SIZE = 64 * 1024 * 1024;
    private static final int MAX_FLUSH_RETRIES_ON_IO_EXCEPTION = 5;
    private static final Duration INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION = Duration.ofSeconds(5);
    static final String S3_BUCKET_NAME = "bucket";
    static final String S3_OBJECT_KEY = "key";
    static final String S3_BUCKET_REFERENCE_NAME = "s3";

    static final String CSV_FILE_HEADER_INFO_NONE = "none";

    private final S3AsyncClient s3AsyncClient;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final Buffer<Record<Event>> buffer;
    private final String expression;
    private final S3SelectSerializationFormatOption serializationFormatOption;
    private final S3SelectResponseHandler s3SelectResponseHandler;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final CompressionType compressionType;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    private final S3SelectCSVOption s3SelectCSVOption;
    private final S3SelectJsonOption s3SelectJsonOption;
    private final String expressionType;
    public S3SelectObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.expression = s3ObjectRequest.getExpression();
        this.serializationFormatOption = s3ObjectRequest.getSerializationFormatOption();
        this.s3AsyncClient = s3ObjectRequest.getS3AsyncClient();
        this.s3SelectResponseHandler = s3ObjectRequest.getS3SelectResponseHandler();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
        this.compressionType = s3ObjectRequest.getCompressionType();
        this.eventConsumer = s3ObjectRequest.getEventConsumer();
        this.s3SelectCSVOption = s3ObjectRequest.getS3SelectCSVOption();
        this.s3SelectJsonOption = s3ObjectRequest.getS3SelectJsonOption();
        this.expressionType = s3ObjectRequest.getExpressionType();
    }

    /**
     * Fetch the S3 object content using s3 select service and parsing and pushing to buffer.
     * @param s3ObjectReference Contains bucket and s3 object details
     *
     * @throws IOException When Input/Output exception occur while processing the data.
     */
    public void parseS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        try{
            selectObjectInBatches(s3ObjectReference);
        } catch (Exception e){
            LOG.error("Unable to process object reference: {}", s3ObjectReference, e);
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            throw new IOException(e);
        }
    }

    private void selectObjectInBatches(final S3ObjectReference s3ObjectReference) throws IOException {
        final InputSerialization inputSerialization = getInputSerializationFormat(serializationFormatOption);
        InputStream inputStreamList;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);

        final long objectSize = getObjectSize(s3ObjectReference);

        long startRange = 0;
        long endRange = Math.min(MAX_S3_OBJECT_CHUNK_SIZE, objectSize);
        while (startRange < objectSize) {
            final ScanRange scanRange = ScanRange.builder()
                    .start(startRange)
                    .end(endRange)
                    .build();
            final SelectObjectContentRequest selectObjectContentRequest = getS3SelectObjRequest(s3ObjectReference, inputSerialization, scanRange);

            s3AsyncClient.selectObjectContent(selectObjectContentRequest, s3SelectResponseHandler).join();
            if (s3SelectResponseHandler.getException() != null)
                throw new IOException(s3SelectResponseHandler.getException());

            final List<SelectObjectContentEventStream> receivedEvents = s3SelectResponseHandler.getS3SelectContentEvents();
            if (!receivedEvents.isEmpty()) {
                inputStreamList = getInputStreamFromResponseHeader(s3SelectResponseHandler);
                parseCompleteStreamFromResponseHeader(s3ObjectReference, bufferAccumulator, inputStreamList);
                s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(bufferAccumulator.getTotalWritten());
                s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
                receivedEvents.clear();
            } else {
                LOG.info("S3 Select returned no events for S3 object {}", s3ObjectReference);
            }

            startRange = endRange;
            endRange += Math.min(MAX_S3_OBJECT_CHUNK_SIZE, objectSize - endRange);
        }
    }

    private Long getObjectSize(final S3ObjectReference s3ObjectReference) {
        final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey())
                .build();
        final HeadObjectResponse headObjectResponse = s3AsyncClient.headObject(headObjectRequest).join();

        return headObjectResponse.contentLength();
    }

    private InputSerialization getInputSerializationFormat(final S3SelectSerializationFormatOption serializationFormatOption) {
        InputSerialization inputSerialization = null;
        switch (serializationFormatOption) {
            case CSV:
                inputSerialization = InputSerialization.builder()
                        .csv( csv -> csv.fileHeaderInfo(s3SelectCSVOption.getFileHeaderInfo())
                                .quoteEscapeCharacter(s3SelectCSVOption.getQuiteEscape())
                                .comments(s3SelectCSVOption.getComments())
                                .allowQuotedRecordDelimiter(
                                        (s3SelectCSVOption.getQuiteEscape() != null ||
                                                s3SelectCSVOption.getComments() != null) ? true : false).build())
                        .compressionType(compressionType).build();
                break;
            case JSON:
                inputSerialization = InputSerialization.builder().json(json -> json.type(s3SelectJsonOption.getType()).build())
                        .compressionType(compressionType).build();
                break;
            case PARQUET:
                inputSerialization = InputSerialization.builder().parquet(p -> p.build())
                        .compressionType(CompressionType.NONE).build();
                break;
        }
        return inputSerialization;
    }

    private InputStream getInputStreamFromResponseHeader(final S3SelectResponseHandler responseHand) {
        final List<InputStream> inputStreams = responseHand.getS3SelectContentEvents().stream()
                .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
                .map(e -> ((RecordsEvent) e).payload().asInputStream()).collect(Collectors.toList());

        return new SequenceInputStream(Collections.enumeration(inputStreams));
    }

    private SelectObjectContentRequest getS3SelectObjRequest(final S3ObjectReference s3ObjectReference,
                                                             final InputSerialization inputSerialization, final ScanRange scanRange) {
        return SelectObjectContentRequest.builder().bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey()).expressionType(expressionType)
                .expression(expression)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization -> outputSerialization.json(SdkBuilder::build))
                .scanRange(scanRange)
                .build();
    }

    private void parseCompleteStreamFromResponseHeader(final S3ObjectReference s3ObjectReference,
                                                       final BufferAccumulator<Record<Event>> bufferAccumulator, final InputStream stream) throws IOException {
        try (final CountingInputStream countingInputStream = new CountingInputStream(stream);
             final InputStreamReader inputStreamReader = new InputStreamReader(countingInputStream);
             final BufferedReader reader = new BufferedReader(inputStreamReader)) {
            Optional<String> selectObjectOptionalString;
            while ((selectObjectOptionalString = Optional.ofNullable(reader.readLine())).isPresent()) {
                final Optional<JsonNode> optionalNode = addingS3Reference(s3ObjectReference,
                        getJsonNode(selectObjectOptionalString.get()));
                if (optionalNode.isPresent()) {
                    Record<Event> eventRecord = new Record<>(JacksonLog.builder().withData(optionalNode.get()).build());
                    try {
                        eventConsumer.accept(eventRecord.getData(), s3ObjectReference);
                        bufferAccumulator.add(eventRecord);
                    } catch (final TimeoutException ex) {
                        flushWithBackoff(bufferAccumulator, MAX_FLUSH_RETRIES_ON_IO_EXCEPTION, INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION);
                    } catch (final Exception ex) {
                        LOG.error("Failed writing S3 objects to buffer due to: {}", ex.getMessage());
                    }

                }
            }
        }
        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private boolean flushWithBackoff(final BufferAccumulator<Record<Event>> bufferAccumulator, final int maxRetries, final Duration initialDelay) {

        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long nextDelay = initialDelay.toMillis();
        int backoffRate = 2;
        boolean flushedSuccessfully;

        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            final ScheduledFuture<Boolean> flushBufferFuture = scheduledExecutorService.schedule(() -> {
                try {
                    bufferAccumulator.flush();
                    return true;
                } catch (final Exception e) {
                    return false;
                }
            }, nextDelay, TimeUnit.MILLISECONDS);

            try {
                flushedSuccessfully = flushBufferFuture.get();
                if (!flushedSuccessfully) {
                    nextDelay *= backoffRate;
                } else {
                    scheduledExecutorService.shutdownNow();
                    return true;
                }
            } catch (ExecutionException e) {
                LOG.warn("Retrying of flushing the buffer accumulator hit an exception: {}", e.getMessage());
            } catch (InterruptedException e) {
                LOG.warn("Retrying of flushing the buffer accumulator was interrupted: {}", e.getMessage());
            }
        }


        LOG.warn("Flushing the bufferAccumulator failed after {} attempts", maxRetries);
        scheduledExecutorService.shutdownNow();
        return false;
    }

    private JsonNode getJsonNode(String selectObjectOptionalString) throws JsonProcessingException {
        final JsonNode recordJsonNode;
        if(serializationFormatOption.equals(S3SelectSerializationFormatOption.CSV) &&
                CSV_FILE_HEADER_INFO_NONE.equals(s3SelectCSVOption.getFileHeaderInfo()))
            recordJsonNode = csvColumNamesRefactorInCaseNoHeader(selectObjectOptionalString);
        else
            recordJsonNode = mapper.readTree(selectObjectOptionalString);
        return recordJsonNode;
    }

    private static ObjectNode getS3BucketObjectReference(final S3ObjectReference s3ObjectReference,
                                                         final ObjectMapper mapper) {
        ObjectNode objNode = mapper.createObjectNode();
        objNode.put(S3_BUCKET_NAME, s3ObjectReference.getBucketName());
        objNode.put(S3_OBJECT_KEY, s3ObjectReference.getKey());
        return objNode;
    }

    private String generateColumnHeader(final int columnNumber) {
        return "column" + columnNumber;
    }

    private JsonNode csvColumNamesRefactorInCaseNoHeader(String line) {
        final Map<String,String> finalJson = new HashMap<>();
        try {
            final JsonParser jsonParser = jsonFactory.createParser(line);
            final Map<String,String> jsonObject = mapper.readValue(jsonParser, Map.class);
            for(int column =0;column < jsonObject.size();column++){
                finalJson.put(generateColumnHeader(column+1),jsonObject.get("_"+(column+1)));
            }
        } catch (final Exception e) {
            LOG.error("Failed to refactor the columns names, Record - {}", line, e);
        }
        return mapper.valueToTree(finalJson);
    }

    private Optional<JsonNode> addingS3Reference(final S3ObjectReference s3ObjectReference, JsonNode recordJsonNode) {
        try {
            final JsonNode node = mapper.valueToTree(recordJsonNode);
            ((ObjectNode) node).set(S3_BUCKET_REFERENCE_NAME, getS3BucketObjectReference(s3ObjectReference, mapper));
            return Optional.of(node);
        } catch (final Exception e) {
            LOG.error("Failed to append the S3 reference to the record. Bucket details - {}", recordJsonNode, e);
        }
        return Optional.empty();
    }
}