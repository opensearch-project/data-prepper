/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.input.CountingInputStream;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
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
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Class responsible for taking an {@link S3SelectResponseHandler} and creating
 * all the necessary {@link Event} objects in the Data Prepper {@link Buffer}.
 */
public class S3SelectObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(S3SelectObjectWorker.class);

    static final long MAX_S3_OBJECT_CHUNK_SIZE = 64 * 1024 * 1024;
    static final String S3_BUCKET_NAME = "bucket";
    static final String S3_OBJECT_KEY = "key";
    static final String S3_BUCKET_REFERENCE_NAME = "s3";

    static final String CSV_FILE_HEADER_INFO_NONE = "none";
    static final String JSON_LINES_TYPE = "LINES";

    private final S3AsyncClient s3AsyncClient;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final Buffer<Record<Event>> buffer;
    private final String expression;
    private final S3SelectSerializationFormatOption serializationFormatOption;
    private final S3SelectResponseHandlerFactory s3SelectResponseHandlerFactory;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final CompressionType compressionType;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    private final S3SelectCSVOption s3SelectCSVOption;
    private final S3SelectJsonOption s3SelectJsonOption;
    private final String expressionType;

    private final BucketOwnerProvider bucketOwnerProvider;

    public S3SelectObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.expression = s3ObjectRequest.getExpression();
        this.serializationFormatOption = s3ObjectRequest.getSerializationFormatOption();
        this.s3AsyncClient = s3ObjectRequest.getS3AsyncClient();
        this.s3SelectResponseHandlerFactory = s3ObjectRequest.getS3SelectResponseHandlerFactory();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
        this.compressionType = s3ObjectRequest.getCompressionType();
        this.eventConsumer = s3ObjectRequest.getEventConsumer();
        this.s3SelectCSVOption = s3ObjectRequest.getS3SelectCSVOption();
        this.s3SelectJsonOption = s3ObjectRequest.getS3SelectJsonOption();
        this.expressionType = s3ObjectRequest.getExpressionType();
        this.bucketOwnerProvider = s3ObjectRequest.getBucketOwnerProvider();
    }

    public void processS3Object(final S3ObjectReference s3ObjectReference,
                              final S3DataSelection dataSelection,
                              final AcknowledgementSet acknowledgementSet,
                              final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                              final String partitionKey) throws IOException {
        try{
            LOG.info("Read S3 object: {}", s3ObjectReference);
            selectObject(s3ObjectReference, acknowledgementSet);
        } catch (Exception e){
            LOG.error("Unable to process object reference: {}", s3ObjectReference, e);
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            throw new IOException(e);
        }
    }

    @Override
    public void deleteS3Object(final S3ObjectReference s3ObjectReference) {
        throw new UnsupportedOperationException("Deleting S3 objects is not supported with S3 select");
    }

    private void selectObject(final S3ObjectReference s3ObjectReference, final AcknowledgementSet acknowledgementSet) throws IOException {
        final InputSerialization inputSerialization = getInputSerializationFormat(serializationFormatOption);
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);

        if (isScanRangeSupported()) {
            selectObjectInBatches(s3ObjectReference, acknowledgementSet, inputSerialization, bufferAccumulator);
        } else {
            final SelectObjectContentRequest selectObjectContentRequest = getS3SelectObjRequest(s3ObjectReference, inputSerialization, null);
            processSelectObjectRequest(selectObjectContentRequest, s3ObjectReference, acknowledgementSet, bufferAccumulator);
        }

        s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
    }

    private boolean isScanRangeSupported() {
        // Supported configurations:
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html#selecting-content-from-objects-contructing-request
        return isScanRangeSupportedForParquetConfiguration() || isScanRangeSupportedForJsonConfiguration() || isScanRangeSupportedForCsvConfiguration();

    }

    private boolean isScanRangeSupportedForParquetConfiguration() {
        return S3SelectSerializationFormatOption.PARQUET.equals(serializationFormatOption);
    }

    private boolean isScanRangeSupportedForJsonConfiguration() {
        return S3SelectSerializationFormatOption.JSON.equals(serializationFormatOption) &&
                Objects.nonNull(s3SelectJsonOption) &&
                s3SelectJsonOption.getType().equalsIgnoreCase(JSON_LINES_TYPE) &&
                CompressionType.NONE.equals(compressionType);
    }

    private boolean isScanRangeSupportedForCsvConfiguration() {
        return S3SelectSerializationFormatOption.CSV.equals(serializationFormatOption) &&
                Objects.nonNull(s3SelectCSVOption) &&
                !isQuotedRecordDelimiterAllowed() &&
                CompressionType.NONE.equals(compressionType);
    }

    private void selectObjectInBatches(final S3ObjectReference s3ObjectReference, final AcknowledgementSet acknowledgementSet,
                                       final InputSerialization inputSerialization, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        final long objectSize = getObjectSize(s3ObjectReference);

        long startRange = 0;
        long endRange = Math.min(MAX_S3_OBJECT_CHUNK_SIZE, objectSize);
        while (startRange < objectSize) {
            final ScanRange scanRange = ScanRange.builder()
                    .start(startRange)
                    .end(endRange)
                    .build();

            final SelectObjectContentRequest selectObjectContentRequest = getS3SelectObjRequest(s3ObjectReference, inputSerialization, scanRange);
            processSelectObjectRequest(selectObjectContentRequest, s3ObjectReference, acknowledgementSet, bufferAccumulator);

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

    private void processSelectObjectRequest(final SelectObjectContentRequest selectObjectContentRequest, final S3ObjectReference s3ObjectReference,
                                            final AcknowledgementSet acknowledgementSet, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        final S3SelectResponseHandler s3SelectResponseHandler = s3SelectResponseHandlerFactory.provideS3SelectResponseHandler();
        s3AsyncClient.selectObjectContent(selectObjectContentRequest, s3SelectResponseHandler).join();
        if (s3SelectResponseHandler.getException() != null)
            throw new IOException(s3SelectResponseHandler.getException());

        final List<SelectObjectContentEventStream> receivedEvents = s3SelectResponseHandler.getS3SelectContentEvents();
        if (!receivedEvents.isEmpty()) {
            final InputStream inputStreamList = getInputStreamFromResponseHeader(s3SelectResponseHandler);
            parseCompleteStreamFromResponseHeader(acknowledgementSet, s3ObjectReference, bufferAccumulator, inputStreamList);
            s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(bufferAccumulator.getTotalWritten());
            receivedEvents.clear();
        } else {
            LOG.info("S3 Select returned no events for S3 object {}", s3ObjectReference);
        }
    }

    private InputSerialization getInputSerializationFormat(final S3SelectSerializationFormatOption serializationFormatOption) {
        InputSerialization inputSerialization = null;
        switch (serializationFormatOption) {
            case CSV:
                inputSerialization = InputSerialization.builder()
                        .csv( csv -> csv.fileHeaderInfo(s3SelectCSVOption.getFileHeaderInfo())
                                .quoteEscapeCharacter(s3SelectCSVOption.getQuiteEscape())
                                .comments(s3SelectCSVOption.getComments())
                                .allowQuotedRecordDelimiter(isQuotedRecordDelimiterAllowed()).build())
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

    private boolean isQuotedRecordDelimiterAllowed() {
        return s3SelectCSVOption.getQuiteEscape() != null || s3SelectCSVOption.getComments() != null;
    }

    private InputStream getInputStreamFromResponseHeader(final S3SelectResponseHandler responseHand) {
        final List<InputStream> inputStreams = responseHand.getS3SelectContentEvents().stream()
                .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
                .map(e -> ((RecordsEvent) e).payload().asInputStream()).collect(Collectors.toList());

        return new SequenceInputStream(Collections.enumeration(inputStreams));
    }

    private SelectObjectContentRequest getS3SelectObjRequest(final S3ObjectReference s3ObjectReference,
                                                             final InputSerialization inputSerialization,
                                                             final ScanRange scanRange) {
        final SelectObjectContentRequest.Builder selectObjectContentRequestBuilder = SelectObjectContentRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey())
                .expressionType(expressionType)
                .expression(expression)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization -> outputSerialization.json(SdkBuilder::build))
                .scanRange(scanRange);
        bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName())
                .ifPresent(selectObjectContentRequestBuilder::expectedBucketOwner);
        return selectObjectContentRequestBuilder.build();
    }

    private void parseCompleteStreamFromResponseHeader(final AcknowledgementSet acknowledgementSet,
                                                       final S3ObjectReference s3ObjectReference,
                                                       final BufferAccumulator<Record<Event>> bufferAccumulator,
                                                       final InputStream stream) throws IOException {
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
                        // Always add record to acknowledgementSet before adding to
                        // buffer because another thread may take and process
                        // buffer contents before the event record is added
                        // to acknowledgement set
                        if (acknowledgementSet != null) {
                            acknowledgementSet.add(eventRecord.getData());
                        }
                        bufferAccumulator.add(eventRecord);
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
