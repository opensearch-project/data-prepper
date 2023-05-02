/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkAction;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkOperationWriter;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperationConverter;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.ClusterSettingsParser;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManagerFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "opensearch", pluginType = Sink.class)
public class OpenSearchSink extends AbstractSink<Record<Event>> {
  public static final String BULKREQUEST_LATENCY = "bulkRequestLatency";
  public static final String BULKREQUEST_ERRORS = "bulkRequestErrors";
  public static final String BULKREQUEST_SIZE_BYTES = "bulkRequestSizeBytes";
  public static final String DYNAMIC_INDEX_DROPPED_EVENTS = "dynamicIndexDroppedEvents";

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);

  private DlqWriter dlqWriter;
  private BufferedWriter dlqFileWriter;
  private final OpenSearchSinkConfiguration openSearchSinkConfig;
  private final IndexManagerFactory indexManagerFactory;
  private RestHighLevelClient restHighLevelClient;
  private IndexManager indexManager;
  private Supplier<AccumulatingBulkRequest> bulkRequestSupplier;
  private BulkRetryStrategy bulkRetryStrategy;
  private final long bulkSize;
  private final IndexType indexType;
  private final String documentIdField;
  private final String routingField;
  private final String action;
  private final String documentRootKey;
  private String configuredIndexAlias;
  private final ReentrantLock lock;

  private final Timer bulkRequestTimer;
  private final Counter bulkRequestErrorsCounter;
  private final Counter dynamicIndexDroppedEvents;
  private final DistributionSummary bulkRequestSizeBytesSummary;
  private OpenSearchClient openSearchClient;
  private ObjectMapper objectMapper;
  private volatile boolean initialized;
  private PluginSetting pluginSetting;

  private FailedBulkOperationConverter failedBulkOperationConverter;

  private DlqProvider dlqProvider;

  @DataPrepperPluginConstructor
  public OpenSearchSink(final PluginSetting pluginSetting,
                        final PluginFactory pluginFactory) {
    super(pluginSetting);
    bulkRequestTimer = pluginMetrics.timer(BULKREQUEST_LATENCY);
    bulkRequestErrorsCounter = pluginMetrics.counter(BULKREQUEST_ERRORS);
    dynamicIndexDroppedEvents = pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS);
    bulkRequestSizeBytesSummary = pluginMetrics.summary(BULKREQUEST_SIZE_BYTES);

    this.openSearchSinkConfig = OpenSearchSinkConfiguration.readESConfig(pluginSetting);
    this.bulkSize = ByteSizeUnit.MB.toBytes(openSearchSinkConfig.getIndexConfiguration().getBulkSize());
    this.indexType = openSearchSinkConfig.getIndexConfiguration().getIndexType();
    this.documentIdField = openSearchSinkConfig.getIndexConfiguration().getDocumentIdField();
    this.routingField = openSearchSinkConfig.getIndexConfiguration().getRoutingField();
    this.action = openSearchSinkConfig.getIndexConfiguration().getAction();
    this.documentRootKey = openSearchSinkConfig.getIndexConfiguration().getDocumentRootKey();
    this.indexManagerFactory = new IndexManagerFactory(new ClusterSettingsParser());
    this.failedBulkOperationConverter = new FailedBulkOperationConverter(pluginSetting.getPipelineName(), pluginSetting.getName(),
        pluginSetting.getName());
    this.initialized = false;
    this.lock = new ReentrantLock(true);
    this.pluginSetting = pluginSetting;

    final Optional<PluginModel> dlqConfig = openSearchSinkConfig.getRetryConfiguration().getDlq();
    if (dlqConfig.isPresent()) {
      final PluginSetting dlqPluginSetting = new PluginSetting(dlqConfig.get().getPluginName(), dlqConfig.get().getPluginSettings());
      dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());
      dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
    }
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
    } catch (Exception e) {
        if (!BulkRetryStrategy.canRetry(e)) {
            LOG.error("Failed to initialize OpenSearch sink with a non-retryable exception.", e);
            this.shutdown();
            throw e;
        }
        LOG.warn("Failed to initialize OpenSearch sink with a retryable exception. ", e);
        closeFiles();
    }
  }

  private void doInitializeInternal() throws IOException {
    LOG.info("Initializing OpenSearch sink");
    restHighLevelClient = openSearchSinkConfig.getConnectionConfiguration().createClient();
    openSearchClient = openSearchSinkConfig.getConnectionConfiguration().createOpenSearchClient(restHighLevelClient);
    configuredIndexAlias = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();
    indexManager = indexManagerFactory.getIndexManager(indexType, openSearchClient, restHighLevelClient,
            openSearchSinkConfig, configuredIndexAlias);
    final String dlqFile = openSearchSinkConfig.getRetryConfiguration().getDlqFile();
    if (dlqFile != null) {
      dlqFileWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } else if (dlqProvider != null) {
      Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
          .add(pluginSetting.getPipelineName())
          .add(pluginSetting.getName()).toString());
      dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
    }
    indexManager.setupIndex();

    bulkRequestSupplier = () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
    final int maxRetries = openSearchSinkConfig.getRetryConfiguration().getMaxRetries();
    bulkRetryStrategy = new BulkRetryStrategy(
            bulkRequest -> openSearchClient.bulk(bulkRequest.getRequest()),
            this::logFailure,
            pluginMetrics,
            maxRetries,
            bulkRequestSupplier,
            pluginSetting);

    objectMapper = new ObjectMapper();
    this.initialized = true;
    LOG.info("Initialized OpenSearch sink");
  }

  @Override
  public boolean isReady() {
    return initialized;
  }

  @Override
  public void doOutput(final Collection<Record<Event>> records) {
    if (records.isEmpty()) {
      return;
    }

    AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = bulkRequestSupplier.get();

    for (final Record<Event> record : records) {
      final Event event = record.getData();
      final SerializedJson document = getDocument(event);
      final Optional<String> docId = document.getDocumentId();
      final Optional<String> routing = document.getRoutingField();
      String indexName = configuredIndexAlias;
      try {
          indexName = indexManager.getIndexName(event.formatString(indexName));
      } catch (IOException e) {
          dynamicIndexDroppedEvents.increment();
          continue;
      }

      BulkOperation bulkOperation;

      if (StringUtils.equalsIgnoreCase(action, BulkAction.CREATE.toString())) {

        final CreateOperation.Builder<Object> createOperationBuilder = new CreateOperation.Builder<>()
                .index(indexName)
                .document(document);

        docId.ifPresent(createOperationBuilder::id);
        routing.ifPresent(createOperationBuilder::routing);

        bulkOperation = new BulkOperation.Builder()
                .create(createOperationBuilder.build())
                .build();

      } else {

        // Default to "index"

        final IndexOperation.Builder<Object> indexOperationBuilder = new IndexOperation.Builder<>()
                .index(indexName)
                .document(document);

        docId.ifPresent(indexOperationBuilder::id);
        routing.ifPresent(indexOperationBuilder::routing);

        bulkOperation = new BulkOperation.Builder()
                .index(indexOperationBuilder.build())
                .build();

      }

      BulkOperationWrapper bulkOperationWrapper = new BulkOperationWrapper(bulkOperation, event.getEventHandle());
      final long estimatedBytesBeforeAdd = bulkRequest.estimateSizeInBytesWithDocument(bulkOperationWrapper);
      if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.getOperationsCount() > 0) {
        flushBatch(bulkRequest);
        bulkRequest = bulkRequestSupplier.get();
      }
      bulkRequest.addOperation(bulkOperationWrapper);
    }

    // Flush the remaining requests
    if (bulkRequest.getOperationsCount() > 0) {
      flushBatch(bulkRequest);
    }

  }

  private SerializedJson getDocument(final Event event) {
    String docId = (documentIdField != null) ? event.get(documentIdField, String.class) : null;
    String routing = (routingField != null) ? event.get(routingField, String.class) : null;

    final String document = DocumentBuilder.build(event, documentRootKey);

    return SerializedJson.fromStringAndOptionals(document, docId, routing);
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

  private void logFailure(final List<FailedBulkOperation> failedBulkOperations, final Throwable failure) {

    final List<DlqObject> dlqObjects = failedBulkOperations.stream()
        .map(failedBulkOperationConverter::convertToDlqObject)
        .collect(Collectors.toList());

    if (dlqFileWriter != null) {
      dlqObjects.forEach(dlqObject -> {
        final FailedDlqData failedDlqData = (FailedDlqData) dlqObject.getFailedData();
        final String message = failure == null ? failedDlqData.getMessage() : failure.getMessage();
        try {
          dlqFileWriter.write(String.format("{\"Document\": [%s], \"failure\": %s}\n",
              BulkOperationWriter.dlqObjectToString(dlqObject), message));
          dlqObject.releaseEventHandle(true);
        } catch (final IOException e) {
          LOG.error(SENSITIVE, "DLQ failure for Document[{}]", dlqObject.getFailedData(), e);
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
          LOG.error(SENSITIVE, "DLQ failure for Document[{}]", dlqObject.getFailedData(), e);
          dlqObject.releaseEventHandle(false);
        });
      }
    } else {
      dlqObjects.forEach(dlqObject -> {
        LOG.warn(SENSITIVE, "Document [{}] has failure. DLQ not configured", dlqObject.getFailedData(), failure);
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
}
