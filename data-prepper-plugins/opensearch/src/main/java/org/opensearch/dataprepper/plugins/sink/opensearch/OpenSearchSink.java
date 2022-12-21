/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
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
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkAction;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.BulkOperationWriter;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
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
import java.util.Optional;
import java.util.function.Supplier;

@DataPrepperPlugin(name = "opensearch", pluginType = Sink.class)
public class OpenSearchSink extends AbstractSink<Record<Event>> {
  public static final String BULKREQUEST_LATENCY = "bulkRequestLatency";
  public static final String BULKREQUEST_ERRORS = "bulkRequestErrors";
  public static final String BULKREQUEST_SIZE_BYTES = "bulkRequestSizeBytes";

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);

  private BufferedWriter dlqWriter;
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
  private String configuredIndexAlias;

  private final Timer bulkRequestTimer;
  private final Counter bulkRequestErrorsCounter;
  private final DistributionSummary bulkRequestSizeBytesSummary;
  private OpenSearchClient openSearchClient;
  private ObjectMapper objectMapper;

  public OpenSearchSink(final PluginSetting pluginSetting) {
    super(pluginSetting);
    bulkRequestTimer = pluginMetrics.timer(BULKREQUEST_LATENCY);
    bulkRequestErrorsCounter = pluginMetrics.counter(BULKREQUEST_ERRORS);
    bulkRequestSizeBytesSummary = pluginMetrics.summary(BULKREQUEST_SIZE_BYTES);

    this.openSearchSinkConfig = OpenSearchSinkConfiguration.readESConfig(pluginSetting);
    this.bulkSize = ByteSizeUnit.MB.toBytes(openSearchSinkConfig.getIndexConfiguration().getBulkSize());
    this.indexType = openSearchSinkConfig.getIndexConfiguration().getIndexType();
    this.documentIdField = openSearchSinkConfig.getIndexConfiguration().getDocumentIdField();
    this.routingField = openSearchSinkConfig.getIndexConfiguration().getRoutingField();
    this.action = openSearchSinkConfig.getIndexConfiguration().getAction();
    this.indexManagerFactory = new IndexManagerFactory();

    try {
      initialize();
    } catch (final IOException e) {
      this.shutdown();
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public void initialize() throws IOException {
    LOG.info("Initializing OpenSearch sink");
    restHighLevelClient = openSearchSinkConfig.getConnectionConfiguration().createClient();
    configuredIndexAlias = openSearchSinkConfig.getIndexConfiguration().getIndexAlias();
    indexManager = indexManagerFactory.getIndexManager(indexType, restHighLevelClient, openSearchSinkConfig, configuredIndexAlias);
    final String dlqFile = openSearchSinkConfig.getRetryConfiguration().getDlqFile();
    if (dlqFile != null) {
      dlqWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    indexManager.setupIndex();

    OpenSearchTransport transport = new RestClientTransport(restHighLevelClient.getLowLevelClient(), new PreSerializedJsonpMapper());
    openSearchClient = new OpenSearchClient(transport);
    bulkRequestSupplier = () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
    bulkRetryStrategy = new BulkRetryStrategy(
            bulkRequest -> openSearchClient.bulk(bulkRequest.getRequest()),
            this::logFailure,
            pluginMetrics,
            bulkRequestSupplier);
    LOG.info("Initialized OpenSearch sink");

    objectMapper = new ObjectMapper();
  }

  @Override
  public void doOutput(final Collection<Record<Event>> records) {
    if (records.isEmpty()) {
      return;
    }

    AccumulatingBulkRequest<BulkOperation, BulkRequest> bulkRequest = bulkRequestSupplier.get();

    for (final Record<Event> record : records) {
      final Event event = record.getData();
      final SerializedJson document = getDocument(event);
      final Optional<String> docId = document.getDocumentId();
      final Optional<String> routing = document.getRoutingField();
      String indexName = configuredIndexAlias;
      try {
          indexName = indexManager.getIndexName(event.formatString(indexName));
      } catch (IOException e) {
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

      final long estimatedBytesBeforeAdd = bulkRequest.estimateSizeInBytesWithDocument(bulkOperation);
      if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.getOperationsCount() > 0) {
        flushBatch(bulkRequest);
        bulkRequest = bulkRequestSupplier.get();
      }
      bulkRequest.addOperation(bulkOperation);
    }

    // Flush the remaining requests
    if (bulkRequest.getOperationsCount() > 0) {
      flushBatch(bulkRequest);
    }

  }

  private SerializedJson getDocument(final Event event) {
    String docId = (documentIdField != null) ? event.get(documentIdField, String.class) : null;
    String routing = (routingField != null) ? event.get(routingField, String.class) : null;
    return SerializedJson.fromStringAndOptionals(event.toJsonString(), docId, routing);
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

  private void logFailure(final BulkOperation bulkOperation, final Throwable failure) {
    if (dlqWriter != null) {
      try {
        dlqWriter.write(String.format("{\"Document\": [%s], \"failure\": %s}\n",
                BulkOperationWriter.bulkOperationToString(bulkOperation), failure.getMessage()));
      } catch (final IOException e) {
        LOG.error("DLQ failed for Document [{}]", bulkOperation, e);
      }
    } else {
      LOG.warn("Document [{}] has failure.", bulkOperation.toString(), failure);
    }
  }

  @Override
  public void shutdown() {
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
  }
}
