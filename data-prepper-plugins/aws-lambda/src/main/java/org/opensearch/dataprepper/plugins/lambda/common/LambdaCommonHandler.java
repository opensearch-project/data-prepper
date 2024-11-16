/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.types.ByteCount;

import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.BiConsumer;

public class LambdaCommonHandler {
    private final Logger LOG;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final String invocationType;
	private final LambdaCommonConfig config;
	private final String whenCondition;
    BufferFactory bufferFactory;
	final InputCodec responseCodec;
	final ExpressionEvaluator expressionEvaluator;
	JsonOutputCodecConfig jsonOutputCodecConfig;
    private final int maxEvents;
    private final ByteCount maxBytes;
    private final Duration maxCollectionDuration;
    private final ResponseEventHandlingStrategy responseStrategy;

	public LambdaCommonHandler(final Logger log,
			final LambdaAsyncClient lambdaAsyncClient,
			final JsonOutputCodecConfig jsonOutputCodecConfig,
			final InputCodec responseCodec,
			final String whenCondition,
			final ExpressionEvaluator expressionEvaluator,
			final ResponseEventHandlingStrategy responseStrategy,
			final LambdaCommonConfig lambdaCommonConfig) {
		this.LOG = log;
        this.lambdaAsyncClient = lambdaAsyncClient;
		this.responseStrategy = responseStrategy;
		this.config = lambdaCommonConfig;
		this.jsonOutputCodecConfig = jsonOutputCodecConfig;
		this.whenCondition = whenCondition;
		this.responseCodec = responseCodec;
		this.expressionEvaluator = expressionEvaluator;
        this.functionName = config.getFunctionName();
        this.invocationType = config.getInvocationType().getAwsLambdaValue();
        maxEvents = lambdaCommonConfig.getBatchOptions().getThresholdOptions().getEventCount();
        maxBytes = lambdaCommonConfig.getBatchOptions().getThresholdOptions().getMaximumSize();
        maxCollectionDuration = lambdaCommonConfig.getBatchOptions().getThresholdOptions().getEventCollectTimeOut();
        bufferFactory = new InMemoryBufferFactory();
	}

    public LambdaCommonHandler(final Logger log,
                               final LambdaAsyncClient lambdaAsyncClient,
                               final JsonOutputCodecConfig jsonOutputCodecConfig,
                               final LambdaCommonConfig lambdaCommonConfig) {
        this(log, lambdaAsyncClient, jsonOutputCodecConfig, null, null, null, null, lambdaCommonConfig);
    }

    public Buffer createBuffer(BufferFactory bufferFactory) {
        try {
            LOG.debug("Resetting buffer");
            return bufferFactory.getBuffer(lambdaAsyncClient, functionName, invocationType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset buffer", e);
        }
    }

    public boolean checkStatusCode(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.error("Lambda invocation returned with non-success status code: {}", statusCode);
            return false;
        }
        return true;
    }

    public void waitForFutures(List<CompletableFuture<Void>> futureList) {
        if (!futureList.isEmpty()) {
            try {
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
                LOG.info("All {} Lambda invocations have completed", futureList.size());
            } catch (Exception e) {
                LOG.warn("Exception while waiting for Lambda invocations to complete", e);
            } finally {
                futureList.clear();
            }
        }
    }

	public List<Record<Event>> sendRecords(Collection<Record<Event>> records,
			BiConsumer<Buffer, List<Record<Event>>> successHandler, BiConsumer<Buffer, List<Record<Event>>> failureHandler) {
		List<Record<Event>> resultRecords = Collections.synchronizedList(new ArrayList());
		boolean createNewBuffer = true;
		Buffer currentBufferPerBatch = null;
		OutputCodec requestCodec = null;
        List futureList = new ArrayList<>();
        for (Record<Event> record : records) {
            final Event event = record.getData();

            // If the condition is false, add the event to resultRecords as-is
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
				synchronized(resultRecords) {
					resultRecords.add(record);
				}
                continue;
            }

			try {
				if (createNewBuffer) {
					currentBufferPerBatch = createBuffer(bufferFactory);
					requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);
					requestCodec.start(currentBufferPerBatch.getOutputStream(), event, new OutputCodecContext());
				}
				requestCodec.writeEvent(event, currentBufferPerBatch.getOutputStream());
			} catch (IOException ex) {
				LOG.error("Failed to start or write to request codec");
				break;
			}
			currentBufferPerBatch.addRecord(record);
			createNewBuffer = flushToLambdaIfNeeded(resultRecords, currentBufferPerBatch,
					requestCodec, futureList, successHandler, failureHandler, false);
		}
        if (!createNewBuffer && currentBufferPerBatch.getEventCount() > 0) {
			flushToLambdaIfNeeded(resultRecords, currentBufferPerBatch,
					requestCodec, futureList, successHandler, failureHandler, true);
		}
        waitForFutures(futureList);
        return resultRecords;
	}

    boolean flushToLambdaIfNeeded(List<Record<Event>> resultRecords, Buffer currentBufferPerBatch,
                               OutputCodec requestCodec, List futureList,
							   BiConsumer<Buffer, List<Record<Event>>> successHandler, BiConsumer<Buffer, List<Record<Event>>> failureHandler,
                               boolean forceFlush) {

        LOG.debug("currentBufferPerBatchEventCount:{}, maxEvents:{}, maxBytes:{}, " +
                "maxCollectionDuration:{}, forceFlush:{} ", currentBufferPerBatch.getEventCount(),
                maxEvents, maxBytes, maxCollectionDuration, forceFlush);
        if (forceFlush || ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes, maxCollectionDuration)) {
            try {
                requestCodec.complete(currentBufferPerBatch.getOutputStream());

                // Capture buffer before resetting
                final int eventCount = currentBufferPerBatch.getEventCount();

                CompletableFuture<InvokeResponse> future = currentBufferPerBatch.flushToLambda(invocationType);

                // Handle future
                CompletableFuture<Void> processingFuture = future.thenAccept(response -> {
                    //Success handler
                    handleLambdaResponse(resultRecords, currentBufferPerBatch, eventCount, response, successHandler, failureHandler);
                }).exceptionally(throwable -> {
                    //Failure handler
                    List<Record<Event>> bufferRecords = currentBufferPerBatch.getRecords();
                    Record<Event> eventRecord = bufferRecords.isEmpty() ? null : bufferRecords.get(0);
                    LOG.error(NOISY, "Exception occurred while invoking Lambda. Function: {} , Event: {}",
                                functionName, eventRecord == null? "null":eventRecord.getData(), throwable);
                    //requestPayloadMetric.set(currentBufferPerBatch.getPayloadRequestSize());
                    //responsePayloadMetric.set(0);
                    Duration latency = currentBufferPerBatch.stopLatencyWatch();
                    //lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                    handleFailure(throwable, currentBufferPerBatch, resultRecords, failureHandler);
                    return null;
                });

                futureList.add(processingFuture);
            } catch (IOException e) {
                LOG.error(NOISY, "Exception while flushing to lambda", e);
                handleFailure(e, currentBufferPerBatch, resultRecords, failureHandler);
            }
            return true;
        }
        return false;
    }

    private void handleLambdaResponse(List<Record<Event>> resultRecords, Buffer flushedBuffer,
                                      int eventCount, InvokeResponse response,
							   BiConsumer<Buffer, List<Record<Event>>> successHandler, BiConsumer<Buffer, List<Record<Event>>> failureHandler) {
        boolean success = checkStatusCode(response);
        if (success) {
            LOG.info("Successfully flushed {} events", eventCount);

            //metrics
            //numberOfRecordsSuccessCounter.increment(eventCount);
            Duration latency = flushedBuffer.stopLatencyWatch();
            //lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
            //totalFlushedEvents += eventCount;

            convertLambdaResponseToEvent(resultRecords, response, flushedBuffer, successHandler);
        } else {
            // Non-2xx status code treated as failure
            handleFailure(new RuntimeException("Non-success Lambda status code: " + response.statusCode()), flushedBuffer, resultRecords, failureHandler);
        }
    }

    /*
     * Assumption: Lambda always returns json array.
     * 1. If response has an array, we assume that we split the individual events.
     * 2. If it is not an array, then create one event per response.
     */
    void convertLambdaResponseToEvent(final List<Record<Event>> resultRecords, final InvokeResponse lambdaResponse,
                                      Buffer flushedBuffer, BiConsumer<Buffer, List<Record<Event>>> successHandler) {
        try {
            List<Event> parsedEvents = new ArrayList<>();
            List<Record<Event>> originalRecords = flushedBuffer.getRecords();

            SdkBytes payload = lambdaResponse.payload();
            // Handle null or empty payload
            if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
                LOG.warn(NOISY, "Lambda response payload is null or empty, dropping the original events");
                // Set metrics
                //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                //responsePayloadMetric.set(0);
            } else {
                // Set metrics
                //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                //responsePayloadMetric.set(payload.asByteArray().length);

                LOG.debug("Response payload:{}", payload.asUtf8String());
                InputStream inputStream = new ByteArrayInputStream(payload.asByteArray());
                //Convert to response codec
                try {
                    responseCodec.parse(inputStream, record -> {
                        Event event = record.getData();
                        parsedEvents.add(event);
                    });
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                LOG.debug("Parsed Event Size:{}, FlushedBuffer eventCount:{}, " +
                        "FlushedBuffer size:{}", parsedEvents.size(), flushedBuffer.getEventCount(),
                        flushedBuffer.getSize());
				synchronized(resultRecords) {
					responseStrategy.handleEvents(parsedEvents, originalRecords, resultRecords, flushedBuffer);
					successHandler.accept(flushedBuffer, originalRecords);
				}
            }
        } catch (Exception e) {
            LOG.error(NOISY, "Error converting Lambda response to Event");
            // Metrics update
            //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
            //responsePayloadMetric.set(0);
            //????? handleFailure(e, flushedBuffer, resultRecords, failureHandler);
        }
    }

    /*
     * If one event in the Buffer fails, we consider that the entire
     * Batch fails and tag each event in that Batch.
     */
    void handleFailure(Throwable e, Buffer flushedBuffer, List<Record<Event>> resultRecords, BiConsumer<Buffer, List<Record<Event>>> failureHandler) {
        try {
            if (flushedBuffer.getEventCount() > 0) {
                //numberOfRecordsFailedCounter.increment(flushedBuffer.getEventCount());
            }
			synchronized(resultRecords) {
				failureHandler.accept(flushedBuffer, resultRecords);
			}

            //addFailureTags(flushedBuffer, resultRecords);
            LOG.error(NOISY, "Failed to process batch due to error: ", e);
        } catch(Exception ex){
            LOG.error(NOISY, "Exception in handleFailure while processing failure for buffer: ", ex);
        }
    }

}
