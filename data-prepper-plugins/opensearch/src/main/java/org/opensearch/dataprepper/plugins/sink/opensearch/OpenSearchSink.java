/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import jakarta.json.stream.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluationException;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.exceptions.EventKeyNotFoundException;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdater;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessNetworkPolicyUpdaterFactory;
import org.opensearch.dataprepper.plugins.common.opensearch.ServerlessOptionsFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.*;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperationConverter;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.ClusterSettingsParser;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManagerFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplateAPIWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplateAPIWrapperFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateStrategy;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

@DataPrepperPlugin(name = "opensearch", pluginType = Sink.class)
public class OpenSearchSink extends AbstractSink<Record<Event>> {
  public static final String BULKREQUEST_LATENCY = "bulkRequestLatency";
  public static final String SEARCHREQUEST_LATENCY = "searchRequestLatency";
  public static final String BULKREQUEST_ERRORS = "bulkRequestErrors";
  public static final String INVALID_ACTION_ERRORS = "invalidActionErrors";
  public static final String BULKREQUEST_SIZE_BYTES = "bulkRequestSizeBytes";
  public static final String DYNAMIC_INDEX_DROPPED_EVENTS = "dynamicIndexDroppedEvents";
  public static final String INVALID_VERSION_EXPRESSION_DROPPED_EVENTS = "dynamicDocumentVersionDroppedEvents";

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);
  private static final int INITIALIZE_RETRY_WAIT_TIME_MS = 5000;
  private final AwsCredentialsSupplier awsCredentialsSupplier;

  private DlqWriter dlqWriter;
  private BufferedWriter dlqFileWriter;
  private final OpenSearchSinkConfiguration openSearchSinkConfig;
  private final IndexManagerFactory indexManagerFactory;
  private RestHighLevelClient restHighLevelClient;
  private IndexManager indexManager;
  private Supplier<AccumulatingBulkRequest> bulkRequestSupplier;
  private BulkRetryStrategy bulkRetryStrategy;
  private SearchRetryStrategy searchRetryStrategy;
  private BulkApiWrapper bulkApiWrapper;
  private final long bulkSize;
  private final long flushTimeout;
  private final IndexType indexType;
  private final String documentIdField;
  private final String documentId;
  private final String routingField;
  private final String routing;
  private final String action;
  private final List<Map<String, Object>> actions;
  private final String documentRootKey;
  private String configuredIndexAlias;
  private final ReentrantLock lock;
  private final VersionType versionType;
  private final String versionExpression;

  private final Timer bulkRequestTimer;
  private final Timer searchRequestTimer;
  private final Counter bulkRequestErrorsCounter;
  private final Counter invalidActionErrorsCounter;
  private final Counter dynamicIndexDroppedEvents;
  private final DistributionSummary bulkRequestSizeBytesSummary;
  private final Counter dynamicDocumentVersionDroppedEvents;
  private OpenSearchClient openSearchClient;
  private OpenSearchClientRefresher openSearchClientRefresher;
  private ObjectMapper objectMapper;
  private volatile boolean initialized;
  private PluginSetting pluginSetting;
  private final SinkContext sinkContext;
  private final ExpressionEvaluator expressionEvaluator;

  private FailedBulkOperationConverter failedBulkOperationConverter;

  private DlqProvider dlqProvider;
  private final ConcurrentHashMap<Long, AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>> bulkRequestMap;
  private final ConcurrentHashMap<Long, Long> lastFlushTimeMap;
  private final PluginConfigObservable pluginConfigObservable;
  private final List<Processor> response_action_processors;
  private final PluginFactory pluginFactory;

  @DataPrepperPluginConstructor
  public OpenSearchSink(final PluginSetting pluginSetting,
                        final PluginFactory pluginFactory,
                        final SinkContext sinkContext,
                        final ExpressionEvaluator expressionEvaluator,
                        final AwsCredentialsSupplier awsCredentialsSupplier,
                        final PluginConfigObservable pluginConfigObservable) {
    super(pluginSetting, Integer.MAX_VALUE, INITIALIZE_RETRY_WAIT_TIME_MS);
    this.awsCredentialsSupplier = awsCredentialsSupplier;
    this.sinkContext = sinkContext != null ? sinkContext : new SinkContext(null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    this.expressionEvaluator = expressionEvaluator;
    bulkRequestTimer = pluginMetrics.timer(BULKREQUEST_LATENCY);
    searchRequestTimer = pluginMetrics.timer(SEARCHREQUEST_LATENCY);
    bulkRequestErrorsCounter = pluginMetrics.counter(BULKREQUEST_ERRORS);
    invalidActionErrorsCounter = pluginMetrics.counter(INVALID_ACTION_ERRORS);
    dynamicIndexDroppedEvents = pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS);
    bulkRequestSizeBytesSummary = pluginMetrics.summary(BULKREQUEST_SIZE_BYTES);
    dynamicDocumentVersionDroppedEvents = pluginMetrics.counter(INVALID_VERSION_EXPRESSION_DROPPED_EVENTS);

    this.openSearchSinkConfig = OpenSearchSinkConfiguration.readESConfig(pluginSetting, expressionEvaluator);
    this.bulkSize = ByteSizeUnit.MB.toBytes(openSearchSinkConfig.getIndexConfiguration().getBulkSize());
    this.flushTimeout = openSearchSinkConfig.getIndexConfiguration().getFlushTimeout();
    this.indexType = openSearchSinkConfig.getIndexConfiguration().getIndexType();
    this.documentIdField = openSearchSinkConfig.getIndexConfiguration().getDocumentIdField();
    this.documentId = openSearchSinkConfig.getIndexConfiguration().getDocumentId();
    this.routingField = openSearchSinkConfig.getIndexConfiguration().getRoutingField();
    this.routing = openSearchSinkConfig.getIndexConfiguration().getRouting();
    this.action = openSearchSinkConfig.getIndexConfiguration().getAction();
    this.actions = openSearchSinkConfig.getIndexConfiguration().getActions();
    this.documentRootKey = openSearchSinkConfig.getIndexConfiguration().getDocumentRootKey();
    this.versionType = openSearchSinkConfig.getIndexConfiguration().getVersionType();
    this.versionExpression = openSearchSinkConfig.getIndexConfiguration().getVersionExpression();
    this.indexManagerFactory = new IndexManagerFactory(new ClusterSettingsParser());
    this.failedBulkOperationConverter = new FailedBulkOperationConverter(pluginSetting.getPipelineName(), pluginSetting.getName(),
        pluginSetting.getName());
    this.initialized = false;
    this.lock = new ReentrantLock(true);
    this.pluginSetting = pluginSetting;
    this.bulkRequestMap = new ConcurrentHashMap<>();
    this.lastFlushTimeMap = new ConcurrentHashMap<>();
    this.pluginConfigObservable = pluginConfigObservable;
    this.pluginFactory = pluginFactory;

    final Optional<PluginModel> dlqConfig = openSearchSinkConfig.getRetryConfiguration().getDlq();
    if (dlqConfig.isPresent()) {
      final PluginSetting dlqPluginSetting = new PluginSetting(dlqConfig.get().getPluginName(), dlqConfig.get().getPluginSettings());
      dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());
      dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
    }
      response_action_processors = new ArrayList<>();
  }

  @Override
  public void doInitialize() {
    try {
        doInitializeInternal();
    } catch (IOException e) {
        LOG.warn("Failed to initialize OpenSearch sink, retrying: {} ", e.getMessage());
        closeFiles();
    } catch (InvalidPluginConfigurationException e) {
        LOG.error("Failed to initialize OpenSearch sink due to a configuration error.", e);
        this.shutdown();
        throw new RuntimeException(e.getMessage(), e);
    } catch (IllegalArgumentException e) {
        LOG.error("Failed to initialize OpenSearch sink due to a configuration error.", e);
        this.shutdown();
        throw e;
    } catch (Exception e) {
        LOG.warn("Failed to initialize OpenSearch sink with a retryable exception. ", e);
        closeFiles();
    }
  }

  private void doInitializeInternal() throws IOException {
    LOG.info("Initializing OpenSearch sink");
    final ConnectionConfiguration connectionConfiguration = openSearchSinkConfig.getConnectionConfiguration();
    restHighLevelClient = connectionConfiguration.createClient(awsCredentialsSupplier);
    openSearchClient = connectionConfiguration.createOpenSearchClient(restHighLevelClient, awsCredentialsSupplier);
    final Function<ConnectionConfiguration, OpenSearchClient> clientFunction =
            (connectionConfiguration1) -> {
      final RestHighLevelClient restHighLevelClient1 = connectionConfiguration1.createClient(awsCredentialsSupplier);
      return connectionConfiguration1.createOpenSearchClient(restHighLevelClient1, awsCredentialsSupplier).withTransportOptions(
              TransportOptions.builder()
                      .setParameter("filter_path", "errors,took,items.*.error,items.*.status,items.*._index,items.*._id")
                      .build());
    };
    openSearchClientRefresher = new OpenSearchClientRefresher(
            pluginMetrics, connectionConfiguration, clientFunction);
    pluginConfigObservable.addPluginConfigObserver(
            newPluginSetting -> openSearchClientRefresher.update((PluginSetting) newPluginSetting));
    configuredIndexAlias = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();
    final IndexTemplateAPIWrapper indexTemplateAPIWrapper = IndexTemplateAPIWrapperFactory.getWrapper(
            openSearchSinkConfig.getIndexConfiguration(), openSearchClient);
    final TemplateStrategy templateStrategy = openSearchSinkConfig.getIndexConfiguration().getTemplateType()
            .createTemplateStrategy(indexTemplateAPIWrapper);
    indexManager = indexManagerFactory.getIndexManager(indexType, openSearchClient, restHighLevelClient,
            openSearchSinkConfig, templateStrategy, configuredIndexAlias);
    final String dlqFile = openSearchSinkConfig.getRetryConfiguration().getDlqFile();
    if (dlqFile != null) {
      dlqFileWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } else if (dlqProvider != null) {
      Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
          .add(pluginSetting.getPipelineName())
          .add(pluginSetting.getName()).toString());
      dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
    }

    sinkContext.getResponseActions().stream().forEach(p -> {
      final PluginSetting dlqPluginSetting = new PluginSetting(p.getPluginName(), p.getPluginSettings());
      dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());
      response_action_processors.add(pluginFactory.loadPlugin(Processor.class, dlqPluginSetting));
      }
    );

    // Attempt to update the serverless network policy if required argument are given.
    maybeUpdateServerlessNetworkPolicy();

    indexManager.setupIndex();

    final Boolean requireAlias = indexManager.isIndexAlias(configuredIndexAlias);
    final boolean isEstimateBulkSizeUsingCompression = openSearchSinkConfig.getIndexConfiguration().isEstimateBulkSizeUsingCompression();
    final boolean isRequestCompressionEnabled = openSearchSinkConfig.getConnectionConfiguration().isRequestCompressionEnabled();
    if (isEstimateBulkSizeUsingCompression && isRequestCompressionEnabled) {
      final int maxLocalCompressionsForEstimation = openSearchSinkConfig.getIndexConfiguration().getMaxLocalCompressionsForEstimation();
      bulkRequestSupplier = () -> new JavaClientAccumulatingCompressedBulkRequest(new BulkRequest.Builder().requireAlias(requireAlias), bulkSize, maxLocalCompressionsForEstimation);
    } else if (isEstimateBulkSizeUsingCompression) {
      LOG.warn("Estimate bulk request size using compression was enabled but request compression is disabled. " +
              "Estimating bulk request size without compression.");
      bulkRequestSupplier = () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder().requireAlias(requireAlias));
    } else {
      bulkRequestSupplier = () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder().requireAlias(requireAlias));
    }

    final int maxRetries = openSearchSinkConfig.getRetryConfiguration().getMaxRetries();
    bulkApiWrapper = BulkApiWrapperFactory.getWrapper(openSearchSinkConfig.getIndexConfiguration(),
            () -> openSearchClientRefresher.get());
    bulkRetryStrategy = new BulkRetryStrategy(bulkRequest -> bulkApiWrapper.bulk(bulkRequest.getRequest()),
            this::logFailureForBulkRequests,
            pluginMetrics,
            maxRetries,
            bulkRequestSupplier,
            pluginSetting);

    this.searchRetryStrategy = new SearchRetryStrategy(restHighLevelClient, openSearchClient, maxRetries);
    objectMapper = new ObjectMapper();
    this.initialized = true;
    LOG.info("Initialized OpenSearch sink");
  }

  double getInvalidActionErrorsCount() {
    return invalidActionErrorsCounter.count();
  }

  @Override
  public boolean isReady() {
    return initialized;
  }

  private BulkOperation getBulkOperationForAction(final String action,
                                                  final SerializedJson document,
                                                  final Long version,
                                                  final String indexName,
                                                  final JsonNode jsonNode) {
    BulkOperation bulkOperation;
    final Optional<String> docId = document.getDocumentId();
    final Optional<String> routing = document.getRoutingField();

    if (StringUtils.equals(action, OpenSearchBulkActions.CREATE.toString())) {
       final CreateOperation.Builder<Object> createOperationBuilder =
         new CreateOperation.Builder<>()
             .index(indexName)
             .document(document);
       docId.ifPresent(createOperationBuilder::id);
       routing.ifPresent(createOperationBuilder::routing);
       bulkOperation = new BulkOperation.Builder()
                           .create(createOperationBuilder.build())
                           .build();
       return bulkOperation;
    }
    if (StringUtils.equals(action, OpenSearchBulkActions.UPDATE.toString()) ||
        StringUtils.equals(action, OpenSearchBulkActions.UPSERT.toString())) {

        JsonNode filteredJsonNode = jsonNode;
        try {
          if (isUsingDocumentFilters()) {
            filteredJsonNode = objectMapper.reader().readTree(document.getSerializedJson());
          }
        } catch (final IOException e) {
          throw new RuntimeException(
                  String.format("An exception occurred while deserializing a document for the %s action: %s", action, e.getMessage()));
        }


          final UpdateOperation.Builder<Object> updateOperationBuilder = (StringUtils.equals(action.toLowerCase(), OpenSearchBulkActions.UPSERT.toString())) ?
              new UpdateOperation.Builder<>()
                  .index(indexName)
                  .document(filteredJsonNode)
                  .upsert(filteredJsonNode)
                  .versionType(versionType)
                  .version(version) :
              new UpdateOperation.Builder<>()
                  .index(indexName)
                  .document(filteredJsonNode)
                  .versionType(versionType)
                  .version(version);
          docId.ifPresent(updateOperationBuilder::id);
          routing.ifPresent(updateOperationBuilder::routing);
          bulkOperation = new BulkOperation.Builder()
                              .update(updateOperationBuilder.build())
                              .build();
          return bulkOperation;
    }
    if (StringUtils.equals(action, OpenSearchBulkActions.DELETE.toString())) {
      final DeleteOperation.Builder deleteOperationBuilder =
        new DeleteOperation.Builder().index(indexName);
      docId.ifPresent(deleteOperationBuilder::id);
      routing.ifPresent(deleteOperationBuilder::routing);
      bulkOperation = new BulkOperation.Builder()
                          .delete(deleteOperationBuilder
                                  .versionType(versionType)
                                  .version(version)
                                  .build())
                          .build();
      return bulkOperation;
    }
    // Default to "index"
    final IndexOperation.Builder<Object> indexOperationBuilder =
      new IndexOperation.Builder<>()
              .index(indexName)
              .document(document)
              .version(version)
              .versionType(versionType);
    docId.ifPresent(indexOperationBuilder::id);
    routing.ifPresent(indexOperationBuilder::routing);
    bulkOperation = new BulkOperation.Builder()
                        .index(indexOperationBuilder.build())
                        .build();
    return bulkOperation;
  }

  @Override
  public void doOutput(final Collection<Record<Event>> records) {
    final long threadId = Thread.currentThread().getId();
    if (!bulkRequestMap.containsKey(threadId)) {
      bulkRequestMap.put(threadId, bulkRequestSupplier.get());
    }
    if (!lastFlushTimeMap.containsKey(threadId)) {
      lastFlushTimeMap.put(threadId, System.currentTimeMillis());
    }

    AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = bulkRequestMap.get(threadId);
    long lastFlushTime = lastFlushTimeMap.get(threadId);

    for (final Record<Event> record : records) {
      final Event event = record.getData();
      final SerializedJson document = getDocument(event);
      String indexName = configuredIndexAlias;
      try {
          indexName = indexManager.getIndexName(event.formatString(indexName, expressionEvaluator));
      } catch (final Exception e) {
          LOG.error("There was an exception when constructing the index name. Check the dlq if configured to see details about the affected Event: {}", e.getMessage());
          dynamicIndexDroppedEvents.increment();
          logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
          continue;
      }

      Long version = null;
      String versionExpressionEvaluationResult = null;
      if (versionExpression != null) {
        try {
          versionExpressionEvaluationResult = event.formatString(versionExpression, expressionEvaluator);
          version = Long.valueOf(event.formatString(versionExpression, expressionEvaluator));
        } catch (final NumberFormatException e) {
          final String errorMessage = String.format(
                  "Unable to convert the result of evaluating document_version '%s' to Long for an Event. The evaluation result '%s' must be a valid Long type", versionExpression, versionExpressionEvaluationResult
          );
          LOG.error(errorMessage);
          logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
          dynamicDocumentVersionDroppedEvents.increment();
        } catch (final RuntimeException e) {
          final String errorMessage = String.format(
                  "There was an exception when evaluating the document_version '%s': %s", versionExpression, e.getMessage());
          LOG.error(errorMessage + " Check the dlq if configured to see more details about the affected Event");
          logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
          dynamicDocumentVersionDroppedEvents.increment();
        }
      }

      String eventAction = action;
      if (actions != null) {
        for (final Map<String, Object> actionEntry: actions) {
            final String condition = (String)actionEntry.get("when");
            eventAction = (String)actionEntry.get("type");
            if (condition != null &&
                expressionEvaluator.evaluateConditional(condition, event)) {
                    break;
            }
        }
      }
      if (eventAction.contains("${")) {
          eventAction = event.formatString(eventAction, expressionEvaluator);
      }
      if (OpenSearchBulkActions.fromOptionValue(eventAction) == null) {
        LOG.error("Unknown action {}, skipping the event", eventAction);
        invalidActionErrorsCounter.increment();
        continue;
      }

      SerializedJson serializedJsonNode = null;
      if (StringUtils.equals(action, OpenSearchBulkActions.UPDATE.toString()) ||
          StringUtils.equals(action, OpenSearchBulkActions.UPSERT.toString()) ||
          StringUtils.equals(action, OpenSearchBulkActions.DELETE.toString())) {
            serializedJsonNode = SerializedJson.fromJsonNode(event.getJsonNode(), document);
      }
      BulkOperation bulkOperation;

      try {
        bulkOperation = getBulkOperationForAction(eventAction, document, version, indexName, event.getJsonNode());
      } catch (final Exception e) {
        LOG.error("An exception occurred while constructing the bulk operation for a document: ", e);
        logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
        continue;
      }

      BulkOperationWrapper bulkOperationWrapper = new BulkOperationWrapper(bulkOperation, event.getEventHandle(), serializedJsonNode);
      final long estimatedBytesBeforeAdd = bulkRequest.estimateSizeInBytesWithDocument(bulkOperationWrapper);
      if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.getOperationsCount() > 0) {
        flushBatch(bulkRequest);
        lastFlushTime = System.currentTimeMillis();
        bulkRequest = bulkRequestSupplier.get();
      }
      bulkRequest.addOperation(bulkOperationWrapper);
    }

    // Flush the remaining requests if flush timeout expired
    if (System.currentTimeMillis() - lastFlushTime > flushTimeout && bulkRequest.getOperationsCount() > 0) {
      flushBatch(bulkRequest);
      lastFlushTime = System.currentTimeMillis();
      bulkRequest = bulkRequestSupplier.get();
    }

    bulkRequestMap.put(threadId, bulkRequest);
    lastFlushTimeMap.put(threadId, lastFlushTime);
  }

  @Override
  public Object doOutputSync(final Collection<Record<Event>> records, boolean isQuery) {

    if (!isQuery) {
      return processBulkRequest(records);
    }

    SearchResponse<ObjectNode> response = (SearchResponse) processQueryRequest(records);
    List<Record<Event>> updatedRecords = response.hits().hits().stream()
            .map(hit -> (Event) JacksonEvent.builder()
                    .withData(hit.source())
                    .withEventMetadataAttributes(Map.of(DOCUMENT_ID_METADATA_ATTRIBUTE_NAME, hit.id(), INDEX_METADATA_ATTRIBUTE_NAME, hit.index()))
                    .withEventType(EventType.DOCUMENT.toString()).build())
            .collect(Collectors.toList())
            .stream().map(Record::new).collect(Collectors.toList());

    Collection<Record<Event>> processRecords = updatedRecords;
    for (final Processor processor : response_action_processors) {
      try {
        processRecords = processor.execute(processRecords);
      } catch (final Exception e) {
        LOG.error("A processor threw an exception. This batch of Events will be dropped, and their EventHandles will be released: ", e);
        processRecords = Collections.emptyList();
        break;
      }
    }

    return response;
  }

  private Object processQueryRequest(final Collection<Record<Event>> records) {
    final Event event = records.stream().findFirst().get().getData();
    return executeSearchRequest(event);
//    SearchRequest searchRequest = new SearchRequest();
//    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//    searchSourceBuilder.query(
//            QueryBuilders.wrapperQuery(requestBody)
//    );
//    searchSourceBuilder.sort(
//            event.getJsonNode().get("sort")
//    );
//    searchRequest.source(searchSourceBuilder);
//    searchRequest.indices(event.getMetadata().getAttribute("opensearch_index").toString());
//    searchRequest.scroll(TimeValue.timeValueMinutes(10));





//    try {
//      return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//    } catch (Exception e) {
//      // TODO: retry for status code 429 of OpenSearchException?
//      LOG.error("Search request failed due to {}", e.getMessage());
//    }
//    // Blank/Dummy response
//    SearchProfileShardResults profileResults = new SearchProfileShardResults(Collections.emptyMap());
//    List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions = new ArrayList();
//    SearchResponseSections searchResponseSections =  new SearchResponseSections(SearchHits.empty(),
//            new Aggregations(new ArrayList<>()), new Suggest(suggestions), false, false, profileResults, 0);
//    return new SearchResponse(searchResponseSections, "scrollId", 1, 1, 0, 1,
//            new ShardSearchFailure[0], new SearchResponse.Clusters(1, 1, 0));
  }

  private  BulkResponse processBulkRequest(final Collection<Record<Event>> records) {
    final long threadId = Thread.currentThread().getId();
    if (!bulkRequestMap.containsKey(threadId)) {
      bulkRequestMap.put(threadId, bulkRequestSupplier.get());
    }
    if (!lastFlushTimeMap.containsKey(threadId)) {
      lastFlushTimeMap.put(threadId, System.currentTimeMillis());
    }

    AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = bulkRequestMap.get(threadId);
    long lastFlushTime = lastFlushTimeMap.get(threadId);

    for (final Record<Event> record : records) {
      final Event event = record.getData();
      final SerializedJson document = getDocument(event);
      String indexName = configuredIndexAlias;
      try {
        indexName = indexManager.getIndexName(event.formatString(indexName, expressionEvaluator));
      } catch (final Exception e) {
        LOG.error("There was an exception when constructing the index name. Check the dlq if configured to see details about the affected Event: {}", e.getMessage());
        dynamicIndexDroppedEvents.increment();
        logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, e.getMessage())), e);
        continue;
      }

      Long version = null;
      String versionExpressionEvaluationResult = null;
      if (versionExpression != null) {
        try {
          versionExpressionEvaluationResult = event.formatString(versionExpression, expressionEvaluator);
          version = Long.valueOf(event.formatString(versionExpression, expressionEvaluator));
        } catch (final NumberFormatException e) {
          final String errorMessage = String.format(
                  "Unable to convert the result of evaluating document_version '%s' to Long for an Event. The evaluation result '%s' must be a valid Long type", versionExpression, versionExpressionEvaluationResult
          );
          LOG.error(errorMessage);
          //TODO: return failure object
          //logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
          dynamicDocumentVersionDroppedEvents.increment();
        } catch (final RuntimeException e) {
          final String errorMessage = String.format(
                  "There was an exception when evaluating the document_version '%s': %s", versionExpression, e.getMessage());
          LOG.error(errorMessage + " Check the dlq if configured to see more details about the affected Event");
          //TODO: return failure object
          //logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
          dynamicDocumentVersionDroppedEvents.increment();
        }
      }
      String eventAction = action;
      if (actions != null) {
        for (final Map<String, Object> actionEntry: actions) {
          final String condition = (String)actionEntry.get("when");
          eventAction = (String)actionEntry.get("type");
          if (condition != null &&
                  expressionEvaluator.evaluateConditional(condition, event)) {
            break;
          }
        }
      }
      if (eventAction.contains("${")) {
        eventAction = event.formatString(eventAction, expressionEvaluator);
      }
      if (OpenSearchBulkActions.fromOptionValue(eventAction) == null) {
        LOG.error("Unknown action {}, skipping the event", eventAction);
        invalidActionErrorsCounter.increment();
        continue;
      }

      SerializedJson serializedJsonNode = null;
      if (StringUtils.equals(action, OpenSearchBulkActions.UPDATE.toString()) ||
              StringUtils.equals(action, OpenSearchBulkActions.UPSERT.toString()) ||
              StringUtils.equals(action, OpenSearchBulkActions.DELETE.toString())) {
        serializedJsonNode = SerializedJson.fromJsonNode(event.getJsonNode(), document);
      }
      BulkOperation bulkOperation;
      try {
        bulkOperation = getBulkOperationForAction(eventAction, document, version, indexName, event.getJsonNode());
      } catch (final Exception e) {
        LOG.error("An exception occurred while constructing the bulk operation for a document: ", e);
        //TODO: return failure object
        //logFailureForDlqObjects(List.of(createDlqObjectFromEvent(event, indexName, errorMessage)), e);
        continue;
      }
      BulkOperationWrapper bulkOperationWrapper = new BulkOperationWrapper(bulkOperation, event.getEventHandle(), serializedJsonNode);
      bulkRequest.addOperation(bulkOperationWrapper);
    }

    // Flush the remaining requests if flush timeout expired

    BulkResponse bulkResponse = null;
    if (bulkRequest.getOperationsCount() > 0) {
      bulkResponse = executeBulkRequest(bulkRequest);
      lastFlushTime = System.currentTimeMillis();
      bulkRequest = bulkRequestSupplier.get();
    }

    bulkRequestMap.put(threadId, bulkRequest);
    lastFlushTimeMap.put(threadId, lastFlushTime);

    return bulkResponse;
  }

  SerializedJson getDocument(final Event event) {
    String docId = null;

    if (Objects.nonNull(documentIdField)) {
      docId = event.get(documentIdField, String.class);
    } else if (Objects.nonNull(documentId)) {
      try {
        docId = event.formatString(documentId, expressionEvaluator);
      } catch (final ExpressionEvaluationException | EventKeyNotFoundException e) {
        LOG.error("Unable to construct document_id with format {}, the document_id will be generated by OpenSearch", documentId, e);
      }
    }

    String routingValue = null;
    if (routingField != null) {
        routingValue = event.get(routingField, String.class);
    } else if (routing != null) {
      try {
        routingValue = event.formatString(routing, expressionEvaluator);
      } catch (final ExpressionEvaluationException | EventKeyNotFoundException e) {
        LOG.error("Unable to construct routing with format {}, the routing will be generated by OpenSearch", routing, e);
      }
    }

    final String document = DocumentBuilder.build(event, documentRootKey, sinkContext.getTagsTargetKey(), sinkContext.getIncludeKeys(), sinkContext.getExcludeKeys());

    return SerializedJson.fromStringAndOptionals(document, docId, routingValue);
  }

  private SearchResponse executeSearchRequest(final Event event) {
    return searchRequestTimer.record(() -> {
      try {
        LOG.debug("Sending data to OpenSearch");
        final JsonpMapper jsonpMapper = new JacksonJsonpMapper();
        final JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(new StringReader(event.getJsonNode().get("query").toString()));

        List<SortOptions> sortOptions = new ArrayList<>();
        if (event.getJsonNode().get("sort") != null) {
          for (Object sort : event.getJsonNode().get("sort")) {
            JsonParser sortQueryJsonParser = jsonpMapper.jsonProvider().createParser(new StringReader(sort.toString()));
            sortOptions.add(SortOptions._DESERIALIZER.deserialize(sortQueryJsonParser, jsonpMapper));
          }
        }

        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(event.getMetadata().getAttribute("opensearch_index").toString())
                .query(Query._DESERIALIZER.deserialize(jsonParser, jsonpMapper))
                .sort(sortOptions)
                .build();

        return searchRetryStrategy.executeSearchRequest(searchRequest);
      } catch (final InterruptedException ex) {
        LOG.error("Unexpected Interrupt:", ex);
        bulkRequestErrorsCounter.increment();
        Thread.currentThread().interrupt();
        return null;
      }
    });
  }

  private BulkResponse executeBulkRequest(AccumulatingBulkRequest accumulatingBulkRequest) {
    return bulkRequestTimer.record(() -> {
      try {
        LOG.debug("Sending data to OpenSearch");
        bulkRequestSizeBytesSummary.record(accumulatingBulkRequest.getEstimatedSizeInBytes());
        return bulkRetryStrategy.executeBulkRequest(accumulatingBulkRequest);
      } catch (final InterruptedException ex) {
        LOG.error("Unexpected Interrupt:", ex);
        bulkRequestErrorsCounter.increment();
        Thread.currentThread().interrupt();
        return null;
      }
    });
  }

  private void flushBatch(AccumulatingBulkRequest accumulatingBulkRequest) {
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

  private void logFailureForBulkRequests(final List<FailedBulkOperation> failedBulkOperations, final Throwable failure) {

    final List<DlqObject> dlqObjects = failedBulkOperations.stream()
        .map(failedBulkOperationConverter::convertToDlqObject)
        .collect(Collectors.toList());

    logFailureForDlqObjects(dlqObjects, failure);
  }

  private void logFailureForDlqObjects(final List<DlqObject> dlqObjects, final Throwable failure) {
    if (dlqFileWriter != null) {
      dlqObjects.forEach(dlqObject -> {
        final FailedDlqData failedDlqData = (FailedDlqData) dlqObject.getFailedData();
        final String message = failure == null ? failedDlqData.getMessage() : failure.getMessage();
        try {
          dlqFileWriter.write(String.format("{\"Document\": [%s], \"failure\": %s}\n",
                  BulkOperationWriter.dlqObjectToString(dlqObject), message));
          dlqObject.releaseEventHandle(true);
        } catch (final IOException e) {
        LOG.error("Failed to write a document to the DLQ", e);
          dlqObject.releaseEventHandle(false);
        }
      });
    } else if (dlqWriter != null) {
      try {
        dlqWriter.write(dlqObjects, pluginSetting.getPipelineName(), pluginSetting.getName());
        dlqObjects.forEach((dlqObject) -> {
          dlqObject.releaseEventHandle(true);
        });
      } catch (final IOException e) {
        dlqObjects.forEach(dlqObject -> {
          LOG.error("Failed to write a document to the DLQ", e);
          dlqObject.releaseEventHandle(false);
        });
      }
    } else {
      dlqObjects.forEach(dlqObject -> {

        final FailedDlqData failedDlqData = (FailedDlqData) dlqObject.getFailedData();

        final String message = failure == null ? failedDlqData.getMessage() : failure.getMessage();
        LOG.warn("Document failed to write to OpenSearch with error code {}. Configure a DLQ to save failed documents. Error: {}", failedDlqData.getStatus(), message);
        dlqObject.releaseEventHandle(false);
      });
    }
  }

  private void closeFiles() {
    // Close the client. This closes the low-level client which will close it for both high-level clients.
    if (restHighLevelClient != null) {
      try {
        restHighLevelClient.close();
      } catch (final IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
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
  }

  @Override
  public void shutdown() {
    super.shutdown();
    closeFiles();
  }

  private void maybeUpdateServerlessNetworkPolicy() {
    final Optional<ServerlessOptions> maybeServerlessOptions = ServerlessOptionsFactory.create(
        openSearchSinkConfig.getConnectionConfiguration());

    if (maybeServerlessOptions.isPresent()) {
      final ServerlessNetworkPolicyUpdater networkPolicyUpdater = ServerlessNetworkPolicyUpdaterFactory.create(
          awsCredentialsSupplier, openSearchSinkConfig.getConnectionConfiguration()
      );
      networkPolicyUpdater.updateNetworkPolicy(
          maybeServerlessOptions.get().getNetworkPolicyName(),
          maybeServerlessOptions.get().getCollectionName(),
          maybeServerlessOptions.get().getVpceId()
      );
    }
  }

  private DlqObject createDlqObjectFromEvent(final Event event,
                                             final String index,
                                             final String message) {
    return DlqObject.builder()
            .withEventHandle(event.getEventHandle())
            .withFailedData(FailedDlqData.builder()
                    .withDocument(event.toJsonString())
                    .withIndex(index)
                    .withMessage(message)
                    .build())
            .withPluginName(pluginSetting.getName())
            .withPipelineName(pluginSetting.getPipelineName())
            .withPluginId(pluginSetting.getName())
            .build();
  }

  /**
   * This function is used for update and upsert bulk actions to determine whether the original JsonNode needs to be filtered down
   * based on the user's sink configuration. If a new parameter manipulates the document before sending to OpenSearch, it needs to be added to
   * this list to get applied for update and upsert actions
   * @return whether the doc
   */
  private boolean isUsingDocumentFilters() {
    return documentRootKey != null ||
            (sinkContext.getIncludeKeys() != null && !sinkContext.getIncludeKeys().isEmpty()) ||
            (sinkContext.getExcludeKeys() != null && !sinkContext.getExcludeKeys().isEmpty()) ||
            sinkContext.getTagsTargetKey() != null;
  }
}
