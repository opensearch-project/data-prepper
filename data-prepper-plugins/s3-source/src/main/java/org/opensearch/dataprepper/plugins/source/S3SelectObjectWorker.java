/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.io.input.CountingInputStream;
import org.opensearch.dataprepper.metrics.PluginMetrics;
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.ParquetInput;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;

/**
 * Class responsible for taking an {@link S3SelectResponseHandler} and creating
 * all the necessary {@link Event} objects in the Data Prepper {@link Buffer}.
 */
public class S3SelectObjectWorker implements S3ObjectHandler {

    private static final Logger LOG = LoggerFactory.getLogger(S3SelectObjectWorker.class);
    static final String S3_OBJECTS_SIZE_PROCESSED = "s3ObjectProcessedBytes";
    static final String S3_OBJECTS_FAILED_METRIC_NAME = "s3ObjectsFailed";
    static final String S3_OBJECTS_SUCCEEDED_METRIC_NAME = "s3ObjectsSucceeded";
    static final String S3_OBJECTS_EVENTS = "s3ObjectsEvents";
    static final String S3_BUCKET_NAME = "bucket";
    static final String S3_OBJECT_KEY = "key";
    static final String S3_BUCKET_REFERENCE_NAME = "s3";
    private S3AsyncClient s3AsyncClient;
    private final DistributionSummary s3SelectSizeProcessedSummary;
    private final DistributionSummary s3SelectEventsSummary;
    private final Counter s3ObjectsSucceededCounter;
    private final Counter s3SelectFailedCounter;
    private Duration bufferTimeout;
    private int numberOfRecordsToAccumulate;
    private Buffer<Record<Event>> buffer;
    private String query;
    private String dataSerilizationFormat;

    public S3SelectObjectWorker(final Buffer<Record<Event>> buffer,
    							final int numberOfRecordsToAccumulate,
    							final Duration bufferTimeout,
                                final PluginMetrics pluginMetrics,
                                final String query,
                                final String dataSerilizationFormat) {
        this.buffer = buffer;
        this.numberOfRecordsToAccumulate = numberOfRecordsToAccumulate;
        this.bufferTimeout = bufferTimeout;
        s3SelectSizeProcessedSummary = pluginMetrics.summary(S3_OBJECTS_SIZE_PROCESSED);
        s3SelectEventsSummary = pluginMetrics.summary(S3_OBJECTS_EVENTS);
        s3SelectFailedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_METRIC_NAME);
        s3ObjectsSucceededCounter = pluginMetrics.counter(S3_OBJECTS_SUCCEEDED_METRIC_NAME);
        this.query = query;
        this.dataSerilizationFormat = dataSerilizationFormat;
    }

    /**
     * Fetch the S3 object content using s3 select service and parsing and pushing to buffer.
     * @param s3ObjectReference 
     * @param sdkClient
     *
     * @throws IOException When Input/Output exception occur while processing the data.
     */
    public void parseS3Object(final S3ObjectReference s3ObjectReference,final SdkClient sdkClient) throws IOException {
    	this.s3AsyncClient = (S3AsyncClient)sdkClient;
        final long totalBytesRead;
        final InputStream inputStreamList;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        final InputSerialization inputSerialization = getInputSerializationFormat(dataSerilizationFormat);
        final SelectObjectContentRequest s3Request = getS3SelectObjRequest(s3ObjectReference, inputSerialization);

        final S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final S3SelectResponseHandler responseHand = callS3SelectObject(s3Request, responseHandler, s3ObjectReference,
                bufferAccumulator);
        List<SelectObjectContentEventStream> receivedEvents = responseHand.getReceivedEvents();
        if (!receivedEvents.isEmpty()) {
                inputStreamList = getInputStreamFromResponseHeader(responseHand);
                totalBytesRead = parseCompleteStreamFromResponseHeader(s3ObjectReference, bufferAccumulator,
                        inputStreamList);
                s3SelectSizeProcessedSummary.record(totalBytesRead);
                s3SelectEventsSummary.record(bufferAccumulator.getTotalWritten());
        }
        s3ObjectsSucceededCounter.increment();
    }

    private InputSerialization getInputSerializationFormat(final String format) {
        InputSerialization inputSerialization = null;
        if (format == null || format.isBlank()) {
            throw new S3SelectException(
                    "data_serialization_format is mandatory configuration if s3 select query defined in pipeline configuration");
        }
        S3SelectSerializationFormatOption serializationFormat = S3SelectSerializationFormatOption
                .valueOf(format.toUpperCase());
        switch (serializationFormat) {
        case CSV:
            inputSerialization = InputSerialization.builder()
                    .csv(csv -> csv.fileHeaderInfo(FileHeaderInfo.USE).build())
                    .compressionType(CompressionType.NONE).build();
            break;
        case JSON:
            inputSerialization = InputSerialization.builder().json(json -> json.type(JSONType.DOCUMENT).build())
                    .compressionType(CompressionType.NONE).build();
            break;
        case PARQUET:
            inputSerialization = InputSerialization.builder().parquet(ParquetInput.builder().build())
                    .compressionType(CompressionType.NONE).build();
            break;
        }
        return inputSerialization;
    }

    private S3SelectResponseHandler callS3SelectObject(final SelectObjectContentRequest s3Request,
            final S3SelectResponseHandler responseHandler, final S3ObjectReference s3ObjectReference,
            final BufferAccumulator<Record<Event>> bufferAccumulator) {
        try {
            s3AsyncClient.selectObjectContent(s3Request, responseHandler).get();
        } catch (final Exception ex) {
            LOG.error("Failed Read data from s3 select. request {} ",s3Request,ex.getCause());
            s3SelectFailedCounter.increment();
        }
        return responseHandler;
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
                .expression(query)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization -> outputSerialization.json(JSONOutput.builder().build()))
                .build();
    }

    private long parseCompleteStreamFromResponseHeader(final S3ObjectReference s3ObjectReference,
            final BufferAccumulator<Record<Event>> bufferAccumulator, final InputStream stream) throws IOException {
        final AtomicLong totalBytesRead = new AtomicLong(0);
        Optional<String> selectObjectOptionalString = Optional.empty();
        try (final CountingInputStream countingInputStream = new CountingInputStream(stream);
                final InputStreamReader inputStreamReader = new InputStreamReader(countingInputStream);
                final BufferedReader reader = new BufferedReader(inputStreamReader);) {
            while ((selectObjectOptionalString = Optional.ofNullable(reader.readLine())).isPresent()) {
                final Optional<JsonNode> optionalNode = addingS3Reference(s3ObjectReference,selectObjectOptionalString.get());
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
        } catch (final Exception e) {
            LOG.error("Failed writing S3 Select objects to buffer.", e);
        }
        return totalBytesRead.get();
    }

    private static ObjectNode getS3BucketObjectReference(final S3ObjectReference s3ObjectReference,
            final ObjectMapper mapper) {
        ObjectNode objNode = mapper.createObjectNode();
        objNode.put(S3_BUCKET_NAME, s3ObjectReference.getBucketName());
        objNode.put(S3_OBJECT_KEY, s3ObjectReference.getKey());
        return objNode;
    }

    private Optional<JsonNode> addingS3Reference(final S3ObjectReference s3ObjectReference, String line) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final JsonNode node = mapper.readTree(line);
            ((ObjectNode) node).set(S3_BUCKET_REFERENCE_NAME, getS3BucketObjectReference(s3ObjectReference, mapper));
            return Optional.ofNullable(node);
        } catch (final Exception e) {
            LOG.error("Failed to append the S3 reference to the record. Bucket details - {}", line, e);
        }
        return Optional.empty();
    }
}