/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluationException;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.event.exceptions.EventKeyNotFoundException;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.SinkForwardRecordsContext;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqProvider;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkApiWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkApiWrapperFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkOperationWriter;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingCompressedBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingUncompressedBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.DlqConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperationConverter;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.CustomDocumentBuilder;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamDetector;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamIndex;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexCache;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Implementation of {@link Ingester} that uses the OpenSearch
 * bulk APIs to ingest data.
 */
public class BulkIngester implements Ingester {
    private static final Logger LOG = LoggerFactory.getLogger(BulkIngester.class);
    private static final String PLUGIN_NAME = "opensearch";

    private final OpenSearchSinkConfiguration openSearchSinkConfig;
    private final ExpressionEvaluator expressionEvaluator;
    private final SinkContext sinkContext;
    private final SinkForwardRecordsContext sinkForwardRecordsContext;
    private final PluginMetrics pluginMetrics;
    private final String pipeline;
    private final EventActionResolver eventActionResolver;

    private final OpenSearchClient openSearchClient;
    private final Supplier<OpenSearchClient> openSearchClientSupplier;
    private final Supplier<IndexManager> indexManagerSupplier;
    private final Supplier<HeadlessPipeline> failurePipelineSupplier;

    private final long bulkSize;
    private final long flushTimeout;
    private final String documentIdField;
    private final String documentId;
    private final String routingField;
    private final String routing;
    private final String documentRootKey;
    private final VersionType versionType;
    private final String versionExpression;
    private final ScriptManager scriptManager;
    private final BulkOperationFactory bulkOperationFactory;
    private final FailedBulkOperationConverter failedBulkOperationConverter;

    private final Timer bulkRequestTimer;
    private final Counter bulkRequestErrorsCounter;
    private final Counter invalidActionErrorsCounter;
    private final Counter dynamicIndexDroppedEvents;
    private final DistributionSummary bulkRequestSizeBytesSummary;
    private final Counter dynamicDocumentVersionDroppedEvents;

    private final ConcurrentHashMap<Long, AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>> bulkRequestMap;
    private final ConcurrentHashMap<Long, Long> lastFlushTimeMap;

    private final DlqProvider dlqProvider;
    private final String dlqFile;
    private final ExecutorService queryExecutorService;
    private final CustomDocumentBuilder customDocumentBuilder;

    private boolean useEventInBulkOperation;

    private DlqWriter dlqWriter;
    private BufferedWriter dlqFileWriter;
    private Supplier<AccumulatingBulkRequest> bulkRequestSupplier;
    private BulkRetryStrategy bulkRetryStrategy;
    private BulkApiWrapper bulkApiWrapper;
    private DataStreamIndex dataStreamIndex;
    private ExistingDocumentQueryManager existingDocumentQueryManager;
    private IndexManager indexManager;
    private String configuredIndexAlias;

    public BulkIngester(final OpenSearchSinkConfiguration openSearchSinkConfig,
                        final ExpressionEvaluator expressionEvaluator,
                        final SinkContext sinkContext,
                        final PluginMetrics pluginMetrics,
                        final String pipeline,
                        final EventActionResolver eventActionResolver,
                        final OpenSearchClient openSearchClient,
                        final Supplier<OpenSearchClient> openSearchClientSupplier,
                        final Supplier<IndexManager> indexManagerSupplier,
                        final Supplier<HeadlessPipeline> failurePipelineSupplier,
                        final CustomDocumentBuilder customDocumentBuilder) {
        this.openSearchSinkConfig = openSearchSinkConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.sinkContext = sinkContext;
        this.sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        this.pluginMetrics = pluginMetrics;
        this.pipeline = pipeline;
        this.eventActionResolver = eventActionResolver;

        this.openSearchClient = openSearchClient;
        this.openSearchClientSupplier = openSearchClientSupplier;
        this.indexManagerSupplier = indexManagerSupplier;
        this.failurePipelineSupplier = failurePipelineSupplier;

        this.bulkSize = org.opensearch.common.unit.ByteSizeUnit.MB.toBytes(
                openSearchSinkConfig.getIndexConfiguration().getBulkSize());
        this.flushTimeout = openSearchSinkConfig.getIndexConfiguration().getFlushTimeout();
        this.documentIdField = openSearchSinkConfig.getIndexConfiguration().getDocumentIdField();
        this.documentId = openSearchSinkConfig.getIndexConfiguration().getDocumentId();
        this.routingField = openSearchSinkConfig.getIndexConfiguration().getRoutingField();
        this.routing = openSearchSinkConfig.getIndexConfiguration().getRouting();
        this.documentRootKey = openSearchSinkConfig.getIndexConfiguration().getDocumentRootKey();
        this.versionType = openSearchSinkConfig.getIndexConfiguration().getVersionType();
        this.versionExpression = openSearchSinkConfig.getIndexConfiguration().getVersionExpression();
        this.scriptManager = new ScriptManager(openSearchSinkConfig.getIndexConfiguration().getScriptConfiguration(),
                expressionEvaluator);
        this.bulkOperationFactory = new BulkOperationFactory(versionType, scriptManager, new ObjectMapper(),
                isUsingDocumentFilters());
        this.failedBulkOperationConverter = new FailedBulkOperationConverter(pipeline, PLUGIN_NAME);

        this.bulkRequestTimer = pluginMetrics.timer(OpenSearchSink.BULKREQUEST_LATENCY);
        this.bulkRequestErrorsCounter = pluginMetrics.counter(OpenSearchSink.BULKREQUEST_ERRORS);
        this.invalidActionErrorsCounter = pluginMetrics.counter(OpenSearchSink.INVALID_ACTION_ERRORS);
        this.dynamicIndexDroppedEvents = pluginMetrics.counter(OpenSearchSink.DYNAMIC_INDEX_DROPPED_EVENTS);
        this.bulkRequestSizeBytesSummary = pluginMetrics.summary(OpenSearchSink.BULKREQUEST_SIZE_BYTES);
        this.dynamicDocumentVersionDroppedEvents = pluginMetrics.counter(
                OpenSearchSink.INVALID_VERSION_EXPRESSION_DROPPED_EVENTS);

        this.bulkRequestMap = new ConcurrentHashMap<>();
        this.lastFlushTimeMap = new ConcurrentHashMap<>();

        final Optional<DlqConfiguration> dlqConfig = openSearchSinkConfig.getRetryConfiguration().getDlq();
        if (dlqConfig.isPresent()) {
            this.dlqProvider = new S3DlqProvider(dlqConfig.get().getS3DlqWriterConfig());
        } else {
            this.dlqProvider = null;
        }
        this.dlqFile = openSearchSinkConfig.getRetryConfiguration().getDlqFile();

        this.customDocumentBuilder = customDocumentBuilder;

        this.queryExecutorService = openSearchSinkConfig.getIndexConfiguration().getQueryTerm() != null ?
                Executors.newSingleThreadExecutor(
                        BackgroundThreadFactory.defaultExecutorThreadFactory("existing-document-query-manager")) : null;
    }

    @Override
    public void initialize() throws IOException {
        this.indexManager = indexManagerSupplier.get();
        final HeadlessPipeline failurePipeline = failurePipelineSupplier.get();
        this.useEventInBulkOperation = (failurePipeline != null || sinkContext.getForwardToPipelines().size() > 0);
        this.configuredIndexAlias = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();

        setupDlq();

        final Boolean requireAlias = indexManager.isIndexAlias(configuredIndexAlias);
        setupBulkRequestSupplier(requireAlias);

        final int maxRetries = openSearchSinkConfig.getRetryConfiguration().getMaxRetries();
        bulkApiWrapper = BulkApiWrapperFactory.getWrapper(openSearchSinkConfig.getIndexConfiguration(),
                openSearchClientSupplier);

        if (queryExecutorService != null) {
            existingDocumentQueryManager = new ExistingDocumentQueryManager(
                    openSearchSinkConfig.getIndexConfiguration(), pluginMetrics, openSearchClient);
            queryExecutorService.submit(existingDocumentQueryManager);
        }

        bulkRetryStrategy = new BulkRetryStrategy(
                bulkRequest -> bulkApiWrapper.bulk(bulkRequest.getRequest()),
                this::logFailureForBulkRequests,
                this::successfulOperationsHandler,
                pluginMetrics,
                maxRetries,
                bulkRequestSupplier,
                pipeline,
                PLUGIN_NAME,
                openSearchSinkConfig.getIndexConfiguration().getQueryOnBulkFailures() ? existingDocumentQueryManager : null,
                isExternalVersionType(versionType));

        final IndexCache indexCache = new IndexCache();
        final DataStreamDetector dataStreamDetector = new DataStreamDetector(openSearchClient, indexCache);
        this.dataStreamIndex = new DataStreamIndex(dataStreamDetector, openSearchSinkConfig.getIndexConfiguration());

        eventActionResolver.setDataStreamSupport(dataStreamDetector, dataStreamIndex);
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        final long threadId = Thread.currentThread().getId();
        if (!bulkRequestMap.containsKey(threadId)) {
            bulkRequestMap.put(threadId, bulkRequestSupplier.get());
        }
        if (!lastFlushTimeMap.containsKey(threadId)) {
            lastFlushTimeMap.put(threadId, System.currentTimeMillis());
        }

        AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = bulkRequestMap.get(threadId);
        long lastFlushTime = lastFlushTimeMap.get(threadId);

        Set<BulkOperationWrapper> documentsReadyForIndexing = new HashSet<>();
        if (openSearchSinkConfig.getIndexConfiguration().getQueryTerm() != null) {
            documentsReadyForIndexing = existingDocumentQueryManager.getAndClearBulkOperationsReadyToIndex();
        }

        if (!documentsReadyForIndexing.isEmpty()) {
            LOG.info("Found {} documents ready for indexing from query manager", documentsReadyForIndexing.size());
        }

        for (final BulkOperationWrapper bulkOperationWrapper : documentsReadyForIndexing) {
            bulkRequest = flushBatch(bulkRequest, bulkOperationWrapper, lastFlushTime);
            bulkRequest.addOperation(bulkOperationWrapper);
        }

        final HeadlessPipeline failurePipeline = failurePipelineSupplier.get();

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            String indexName = configuredIndexAlias;
            try {
                indexName = indexManager.getIndexName(event.formatString(indexName, expressionEvaluator));
            } catch (final Exception e) {
                LOG.error(NOISY,
                        "There was an exception when constructing the index name. Check the dlq if configured to see details about the affected Event: {}",
                        e.getMessage());
                dynamicIndexDroppedEvents.increment();
                logFailureForDlqObjects(failurePipeline, List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
                continue;
            }

            dataStreamIndex.ensureTimestamp(event, indexName);

            if (customDocumentBuilder != null) {
                try {
                    final List<String> tsdbDocs = customDocumentBuilder.buildDocuments(event);
                    final String tsdbAction = eventActionResolver.resolveAction(event, indexName);
                    final EventHandle eventHandle = event.getEventHandle();
                    if (tsdbDocs.size() > 1 && eventHandle instanceof InternalEventHandle) {
                        for (int i = 0; i < tsdbDocs.size() - 1; i++) {
                            ((InternalEventHandle) eventHandle).acquireReference();
                        }
                    }
                    for (final String tsdbDoc : tsdbDocs) {
                        final SerializedJson doc = SerializedJson.fromStringAndOptionals(tsdbDoc, null, null, null);
                        final BulkOperation op = bulkOperationFactory.create(tsdbAction, doc, null, indexName, null);
                        final BulkOperationWrapper wrapper = new BulkOperationWrapper(op, eventHandle, null, null);
                        bulkRequest = flushBatch(bulkRequest, wrapper, lastFlushTime);
                        bulkRequest.addOperation(wrapper);
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to build TSDB documents for event: {}", e.getMessage(), e);
                    dynamicIndexDroppedEvents.increment();
                    logFailureForDlqObjects(failurePipelineSupplier.get(), List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
                }
                continue;
            }

            final SerializedJson document = getDocument(event);

            Long version = null;
            String versionExpressionEvaluationResult = null;
            if (versionExpression != null) {
                try {
                    versionExpressionEvaluationResult = event.formatString(versionExpression, expressionEvaluator);
                    version = Long.valueOf(event.formatString(versionExpression, expressionEvaluator));
                } catch (final NumberFormatException e) {
                    final String errorMessage = String.format(
                            "Unable to convert the result of evaluating document_version '%s' to Long for an Event. The evaluation result '%s' must be a valid Long type",
                            versionExpression, versionExpressionEvaluationResult);
                    LOG.error(errorMessage);
                    logFailureForDlqObjects(failurePipeline, List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
                    dynamicDocumentVersionDroppedEvents.increment();
                    continue;
                } catch (final RuntimeException e) {
                    final String errorMessage = String.format(
                            "There was an exception when evaluating the document_version '%s': %s",
                            versionExpression, e.getMessage());
                    LOG.error(errorMessage
                            + " Check the dlq if configured to see more details about the affected Event");
                    logFailureForDlqObjects(failurePipeline, List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
                    dynamicDocumentVersionDroppedEvents.increment();
                    continue;
                }
            }

            final String eventAction = eventActionResolver.resolveAction(event, indexName);
            if (!eventActionResolver.isValidAction(eventAction)) {
                LOG.error("Unknown action {}, skipping the event", eventAction);
                invalidActionErrorsCounter.increment();
                continue;
            }

            SerializedJson serializedJsonNode = null;
            if (StringUtils.equals(eventAction, OpenSearchBulkActions.UPDATE.toString()) ||
                    StringUtils.equals(eventAction, OpenSearchBulkActions.UPSERT.toString()) ||
                    StringUtils.equals(eventAction, OpenSearchBulkActions.DELETE.toString())) {
                serializedJsonNode = SerializedJson.fromJsonNode(event.getJsonNode(), document);
            }
            BulkOperation bulkOperation;

            try {
                bulkOperation = bulkOperationFactory.create(eventAction, document, version, indexName,
                        event.getJsonNode());
            } catch (final Exception e) {
                LOG.error("An exception occurred while constructing the bulk operation for a document: ", e);
                logFailureForDlqObjects(failurePipeline, List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
                continue;
            }

            final String queryTermKey = openSearchSinkConfig.getIndexConfiguration().getQueryTerm();
            final String termValue = queryTermKey != null ?
                    event.get(queryTermKey, String.class) : null;
            BulkOperationWrapper bulkOperationWrapper = (useEventInBulkOperation) ?
                    new BulkOperationWrapper(bulkOperation, event, serializedJsonNode, termValue) :
                    new BulkOperationWrapper(bulkOperation, event.getEventHandle(), serializedJsonNode, termValue);

            if (openSearchSinkConfig.getIndexConfiguration().getQueryWhen() != null &&
                    expressionEvaluator.evaluateConditional(
                            openSearchSinkConfig.getIndexConfiguration().getQueryWhen(), event)) {
                existingDocumentQueryManager.addBulkOperation(bulkOperationWrapper);
                continue;
            }

            bulkRequest = flushBatch(bulkRequest, bulkOperationWrapper, lastFlushTime);
            bulkRequest.addOperation(bulkOperationWrapper);
        }

        if (System.currentTimeMillis() - lastFlushTime > flushTimeout && bulkRequest.getOperationsCount() > 0) {
            flushBatch(bulkRequest);
            lastFlushTime = System.currentTimeMillis();
            bulkRequest = bulkRequestSupplier.get();
        }

        bulkRequestMap.put(threadId, bulkRequest);
        lastFlushTimeMap.put(threadId, lastFlushTime);
    }

    @Override
    public void shutdown() {
        if (dlqWriter != null) {
            try {
                dlqWriter.close();
            } catch (final IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        if (dlqFileWriter != null) {
            try {
                dlqFileWriter.close();
            } catch (final IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        if (queryExecutorService != null && existingDocumentQueryManager != null) {
            existingDocumentQueryManager.stop();
            queryExecutorService.shutdown();
        }
    }

    @VisibleForTesting
    SerializedJson getDocument(final Event event) {
        String docId = null;

        if (documentIdField != null) {
            docId = event.get(documentIdField, String.class);
        } else if (documentId != null) {
            try {
                docId = event.formatString(documentId, expressionEvaluator);
            } catch (final ExpressionEvaluationException | EventKeyNotFoundException e) {
                LOG.error("Unable to construct document_id with format {}, the document_id will be generated by OpenSearch",
                        documentId, e);
            }
        }

        String routingValue = null;
        if (routingField != null) {
            routingValue = event.get(routingField, String.class);
        } else if (routing != null) {
            try {
                routingValue = event.formatString(routing, expressionEvaluator);
            } catch (final ExpressionEvaluationException | EventKeyNotFoundException e) {
                LOG.error("Unable to construct routing with format {}, the routing will be generated by OpenSearch",
                        routing, e);
            }
        }

        final String document = DocumentBuilder.build(event, documentRootKey, sinkContext.getTagsTargetKey(),
                sinkContext.getIncludeKeys(), sinkContext.getExcludeKeys());

        return SerializedJson.builder()
                .withJsonString(document)
                .withDocumentId(docId)
                .withRoutingField(routingValue)
                .withResolvedScriptParameters(scriptManager.resolveParams(event))
                .build();
    }

    @VisibleForTesting
    void successfulOperationsHandler(final List<BulkOperationWrapper> successfulOperations) {
        if (successfulOperations.size() == 0) {
            return;
        }
        if (sinkContext.getForwardToPipelines().size() == 0) {
            for (final BulkOperationWrapper bulkOperation : successfulOperations) {
                if (bulkOperation.getEvent() != null) {
                    bulkOperation.getEvent().getEventHandle().release(true);
                } else {
                    bulkOperation.releaseEventHandle(true);
                }
            }
            return;
        }
        for (final BulkOperationWrapper bulkOperation : successfulOperations) {
            sinkForwardRecordsContext.addRecord(new Record<>(bulkOperation.getEvent()));
        }
        sinkContext.forwardRecords(sinkForwardRecordsContext, null, null);
    }

    private void flushBatch(final AccumulatingBulkRequest accumulatingBulkRequest) {
        bulkRequestTimer.record(() -> {
            try {
                LOG.debug("Sending data to OpenSearch");
                bulkRetryStrategy.execute(accumulatingBulkRequest);
                bulkRequestSizeBytesSummary.record(accumulatingBulkRequest.getEstimatedSizeInBytes());
            } catch (final InterruptedException e) {
                LOG.error("Unexpected Interrupt:", e);
                bulkRequestErrorsCounter.increment();
                Thread.currentThread().interrupt();
            }
        });
    }

    private AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> flushBatch(
            final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest,
            final BulkOperationWrapper bulkOperationWrapper,
            long lastFlushTime) {
        final long estimatedBytesBeforeAdd = bulkRequest.estimateSizeInBytesWithDocument(bulkOperationWrapper);
        if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.getOperationsCount() > 0) {
            flushBatch(bulkRequest);
            lastFlushTime = System.currentTimeMillis();
            return bulkRequestSupplier.get();
        }
        return bulkRequest;
    }

    private void logFailureForBulkRequests(final List<FailedBulkOperation> failedBulkOperations,
                                           final Throwable failure) {
        final List<DlqObject> dlqObjects = failedBulkOperations.stream()
                .map(failedBulkOperationConverter::convertToDlqObject)
                .collect(Collectors.toList());
        logFailureForDlqObjects(failurePipelineSupplier.get(), dlqObjects, failure);
    }

    private void logFailureForDlqObjects(final HeadlessPipeline failurePipeline,
                                         final List<DlqObject> dlqObjects, final Throwable failure) {
        if (failurePipeline != null) {
            List<Record<Event>> records = new ArrayList<>();
            for (DlqObject dlqObject : dlqObjects) {
                Event event = dlqObject.getEvent();
                if (event != null) {
                    event.updateFailureMetadata()
                            .withPluginId(dlqObject.getPluginId())
                            .withPluginName(dlqObject.getPluginName())
                            .withPipelineName(dlqObject.getPipelineName())
                            .withErrorMessage(((FailedDlqData) dlqObject.getFailedData()).getMessage())
                            .with("status", ((FailedDlqData) dlqObject.getFailedData()).getStatus())
                            .with("index", ((FailedDlqData) dlqObject.getFailedData()).getIndex())
                            .with("indexId", ((FailedDlqData) dlqObject.getFailedData()).getIndexId());
                    records.add(new Record<>(event));
                }
            }
            failurePipeline.sendEvents(records);
            return;
        }
        if (dlqFileWriter != null) {
            dlqObjects.forEach(dlqObject -> {
                final FailedDlqData failedDlqData = (FailedDlqData) dlqObject.getFailedData();
                final String message = failure == null ? failedDlqData.getMessage() : failure.getMessage();
                try {
                    dlqFileWriter.write(String.format("{\"Document\": [%s], \"failure\": %s}\n",
                            BulkOperationWriter.dlqObjectToString(dlqObject), message));
                    dlqObject.releaseEventHandle(true);
                } catch (final IOException e) {
                    LOG.error(NOISY, "Failed to write a document to the DLQ", e);
                    dlqObject.releaseEventHandle(false);
                }
            });
        } else if (dlqWriter != null) {
            try {
                dlqWriter.write(dlqObjects, pipeline, PLUGIN_NAME);
                dlqObjects.forEach(dlqObject -> dlqObject.releaseEventHandle(true));
            } catch (final IOException e) {
                dlqObjects.forEach(dlqObject -> {
                    LOG.error(NOISY, "Failed to write a document to the DLQ", e);
                    dlqObject.releaseEventHandle(false);
                });
            }
        } else {
            dlqObjects.forEach(dlqObject -> {
                final FailedDlqData failedDlqData = (FailedDlqData) dlqObject.getFailedData();
                final String message = failure == null ? failedDlqData.getMessage() : failure.getMessage();
                LOG.warn("Document failed to write to OpenSearch with error code {}. Configure a DLQ to save failed documents. Error: {}",
                        failedDlqData.getStatus(), message);
                dlqObject.releaseEventHandle(false);
            });
        }
    }

    private DlqObject createDlqObjectFromEvent(final Event event,
                                               final String index,
                                               final String message) {
        DlqObject.Builder builder = DlqObject.builder()
                .withFailedData(FailedDlqData.builder()
                        .withDocument(event.toJsonString())
                        .withIndex(index)
                        .withMessage(message != null ? message : "")
                        .build())
                .withPluginName(PLUGIN_NAME)
                .withPipelineName(pipeline)
                .withPluginId(PLUGIN_NAME);

        if (useEventInBulkOperation) {
            builder.withEvent(event);
        } else {
            builder.withEventHandle(event.getEventHandle());
        }
        return builder.build();
    }

    private boolean isUsingDocumentFilters() {
        return documentRootKey != null ||
                (sinkContext.getIncludeKeys() != null && !sinkContext.getIncludeKeys().isEmpty()) ||
                (sinkContext.getExcludeKeys() != null && !sinkContext.getExcludeKeys().isEmpty()) ||
                sinkContext.getTagsTargetKey() != null;
    }

    private static boolean isExternalVersionType(final VersionType versionType) {
        return versionType != null && (versionType == VersionType.External || versionType == VersionType.ExternalGte);
    }

    private void setupDlq() {
        if (dlqFile != null) {
            try {
                dlqFileWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (final IOException e) {
                throw new RuntimeException("Failed to open DLQ file: " + dlqFile, e);
            }
        } else if (dlqProvider != null) {
            Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
                    .add(pipeline)
                    .add(PLUGIN_NAME).toString());
            dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        }
    }

    private void setupBulkRequestSupplier(final Boolean requireAlias) {
        final boolean isEstimateBulkSizeUsingCompression =
                openSearchSinkConfig.getIndexConfiguration().isEstimateBulkSizeUsingCompression();
        final boolean isRequestCompressionEnabled =
                openSearchSinkConfig.getConnectionConfiguration().isRequestCompressionEnabled();
        if (isEstimateBulkSizeUsingCompression && isRequestCompressionEnabled) {
            final int maxLocalCompressionsForEstimation =
                    openSearchSinkConfig.getIndexConfiguration().getMaxLocalCompressionsForEstimation();
            bulkRequestSupplier = () -> new JavaClientAccumulatingCompressedBulkRequest(
                    new BulkRequest.Builder().requireAlias(requireAlias), bulkSize,
                    maxLocalCompressionsForEstimation);
        } else {
            if (isEstimateBulkSizeUsingCompression) {
                LOG.warn("Estimate bulk request size using compression was enabled but request compression is disabled. "
                        + "Estimating bulk request size without compression.");
            }
            bulkRequestSupplier = () -> new JavaClientAccumulatingUncompressedBulkRequest(
                    new BulkRequest.Builder().requireAlias(requireAlias));
        }
    }
}
