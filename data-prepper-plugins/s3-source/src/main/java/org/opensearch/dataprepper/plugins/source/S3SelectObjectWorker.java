/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.io.input.CountingInputStream;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.utils.builder.SdkBuilder;

/**
 * Class responsible for taking an {@link S3SelectResponseHandler} and creating
 * all the necessary {@link Event} objects in the Data Prepper {@link Buffer}.
 */
public class S3SelectObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(S3SelectObjectWorker.class);
    static final String S3_BUCKET_NAME = "bucket";
    static final String S3_OBJECT_KEY = "key";
    static final String S3_BUCKET_REFERENCE_NAME = "s3";
    private final S3AsyncClient s3AsyncClient;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final Buffer<Record<Event>> buffer;
    private final String queryStatement;
    private final S3SelectSerializationFormatOption serializationFormatOption;
    private final S3SelectResponseHandler s3SelectResponseHandler;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final CompressionType compressionType;
    private final FileHeaderInfo fileHeaderInfo;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    public S3SelectObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.queryStatement = s3ObjectRequest.getQueryStatement();
        this.serializationFormatOption = s3ObjectRequest.getSerializationFormatOption();
        this.s3AsyncClient = s3ObjectRequest.getS3AsyncClient();
        this.s3SelectResponseHandler = s3ObjectRequest.getS3SelectResponseHandler();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
        this.compressionType = s3ObjectRequest.getCompressionType();
        this.fileHeaderInfo = s3ObjectRequest.getFileHeaderInfo();
    }

    /**
     * Fetch the S3 object content using s3 select service and parsing and pushing to buffer.
     * @param s3ObjectReference Contains bucket and s3 object details
     *
     * @throws IOException When Input/Output exception occur while processing the data.
     */
    public void parseS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        try{
            selectObjectContent(s3ObjectReference);
        }catch(Exception e){
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            throw new IOException(e);
        }
    }

    private void selectObjectContent(S3ObjectReference s3ObjectReference) throws IOException {
        final long totalBytesRead;
        final InputStream inputStreamList;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        final InputSerialization inputSerialization = getInputSerializationFormat(serializationFormatOption
                ,compressionType,fileHeaderInfo);
        final SelectObjectContentRequest selectObjectContentRequest = getS3SelectObjRequest(s3ObjectReference, inputSerialization);
        s3AsyncClient.selectObjectContent(selectObjectContentRequest, s3SelectResponseHandler).join();
        final List<SelectObjectContentEventStream> receivedEvents = s3SelectResponseHandler.getReceivedEvents();
        if (receivedEvents.isEmpty()) {
            throw new IOException("No Records Found.");
        }
        inputStreamList = getInputStreamFromResponseHeader(s3SelectResponseHandler);
        totalBytesRead = parseCompleteStreamFromResponseHeader(s3ObjectReference, bufferAccumulator,
                inputStreamList);

        s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary().record(totalBytesRead);
        s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(bufferAccumulator.getTotalWritten());
        s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
        receivedEvents.clear();
    }

    private InputSerialization getInputSerializationFormat(final S3SelectSerializationFormatOption serializationFormatOption,
                                                           final CompressionType compressionType,
                                                           final FileHeaderInfo fileHeaderInfo) {
        InputSerialization inputSerialization = null;
        switch (serializationFormatOption) {
            case CSV:
                inputSerialization = InputSerialization.builder()
                        .csv( csv -> csv.fileHeaderInfo(fileHeaderInfo).build()).compressionType(compressionType).build();
                break;
            case JSON:
                inputSerialization = InputSerialization.builder().json(json -> json.type(JSONType.DOCUMENT).build())
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
        return responseHand.getReceivedEvents().stream()
                .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
                .map(e -> ((RecordsEvent) e).payload().asInputStream()).collect(Collectors.toList()).stream()
                .reduce(SequenceInputStream::new).orElse(null);
    }

    private SelectObjectContentRequest getS3SelectObjRequest(final S3ObjectReference s3ObjectReference,
                                                             final InputSerialization inputSerialization) {
        return SelectObjectContentRequest.builder().bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey()).expressionType(ExpressionType.SQL)
                .expression(queryStatement)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization -> outputSerialization.json(SdkBuilder::build))
                .build();
    }

    private long parseCompleteStreamFromResponseHeader(final S3ObjectReference s3ObjectReference,
                                                       final BufferAccumulator<Record<Event>> bufferAccumulator,final InputStream stream) throws IOException {
        final AtomicLong totalBytesRead = new AtomicLong(0);
        try (final CountingInputStream countingInputStream = new CountingInputStream(stream);
             final InputStreamReader inputStreamReader = new InputStreamReader(countingInputStream);
             final BufferedReader reader = new BufferedReader(inputStreamReader)) {
            Optional<String> selectObjectOptionalString;
            while ((selectObjectOptionalString = Optional.ofNullable(reader.readLine())).isPresent()) {
                final Optional<JsonNode> optionalNode = addingS3Reference(s3ObjectReference,
                        getJsonNode(selectObjectOptionalString.get()));
                if (optionalNode.isPresent()){
                    Record<Event> eventRecord = new Record<>(JacksonLog.builder().withData(optionalNode.get()).build());
                    try{
                        bufferAccumulator.add(eventRecord);
                    }catch(final Exception ex){
                        throw new IOException(ex);
                    }
                }
            }
            totalBytesRead.addAndGet(countingInputStream.getByteCount());
        }
        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return totalBytesRead.get();
    }

    private JsonNode getJsonNode(String selectObjectOptionalString) throws JsonProcessingException {
        final JsonNode recordJsonNode;
        if(serializationFormatOption.equals(S3SelectSerializationFormatOption.CSV) &&
                FileHeaderInfo.NONE.name().equals(fileHeaderInfo.name()))
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
            for(int i =0;i < jsonObject.size();i++){
                finalJson.put(generateColumnHeader(i+1),jsonObject.get("_"+(i+1)));
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