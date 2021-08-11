/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.AbstractSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexTemplatesRequest;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.indices.IndexTemplateMetadata;
import org.opensearch.client.indices.IndexTemplatesExistRequest;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@DataPrepperPlugin(name = "opensearch", type = PluginType.SINK)
public class OpenSearchSink extends AbstractSink<Record<String>> {
  public static final String BULKREQUEST_LATENCY = "bulkRequestLatency";
  public static final String BULKREQUEST_ERRORS = "bulkRequestErrors";

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);
  // Pulled from BulkRequest to make estimation of bytes consistent
  private static final int REQUEST_OVERHEAD = 50;
  protected static final String INDEX_ALIAS_USED_AS_INDEX_ERROR = "Invalid alias name [%s], an index exists with the same name as the alias";

  private BufferedWriter dlqWriter;
  private final OpenSearchSinkConfiguration esSinkConfig;
  private RestHighLevelClient restHighLevelClient;
  private Supplier<BulkRequest> bulkRequestSupplier;
  private BulkRetryStrategy bulkRetryStrategy;
  private final long bulkSize;
  private final String indexType;
  private final String documentIdField;

  private final Timer bulkRequestTimer;
  private final Counter bulkRequestErrorsCounter;

  public OpenSearchSink(final PluginSetting pluginSetting) {
    super(pluginSetting);
    bulkRequestTimer = pluginMetrics.timer(BULKREQUEST_LATENCY);
    bulkRequestErrorsCounter = pluginMetrics.counter(BULKREQUEST_ERRORS);

    this.esSinkConfig = OpenSearchSinkConfiguration.readESConfig(pluginSetting);
    this.bulkSize = ByteSizeUnit.MB.toBytes(esSinkConfig.getIndexConfiguration().getBulkSize());
    this.indexType = esSinkConfig.getIndexConfiguration().getIndexType();
    this.documentIdField = esSinkConfig.getIndexConfiguration().getDocumentIdField();
    try {
      initialize();
    } catch (final IOException e) {
      this.shutdown();
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public void initialize() throws IOException {
    LOG.info("Initializing OpenSearch sink");
    restHighLevelClient = esSinkConfig.getConnectionConfiguration().createClient();
    final boolean isISMEnabled = IndexStateManagement.checkISMEnabled(restHighLevelClient);
    final Optional<String> policyIdOptional = isISMEnabled ? IndexStateManagement.checkAndCreatePolicy(restHighLevelClient, indexType) :
            Optional.empty();
    if (!esSinkConfig.getIndexConfiguration().getIndexTemplate().isEmpty()) {
      checkAndCreateIndexTemplate(isISMEnabled, policyIdOptional.orElse(null));
    }
    final String dlqFile = esSinkConfig.getRetryConfiguration().getDlqFile();
    if (dlqFile != null) {
      dlqWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    checkAndCreateIndex();
    bulkRequestSupplier = () -> new BulkRequest(esSinkConfig.getIndexConfiguration().getIndexAlias());
    bulkRetryStrategy = new BulkRetryStrategy(
            bulkRequest -> restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT),
            this::logFailure,
            pluginMetrics,
            bulkRequestSupplier);
    LOG.info("Initialized OpenSearch sink");
  }

  @Override
  public void doOutput(final Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return;
    }
    BulkRequest bulkRequest = bulkRequestSupplier.get();
    for (final Record<String> record : records) {
      final String document = record.getData();
      final IndexRequest indexRequest = new IndexRequest().source(document, XContentType.JSON);
      try {
        final Map<String, Object> source = getMapFromJson(document);
        final String docId = (String) source.get(documentIdField);
        if (docId != null) {
          indexRequest.id(docId);
        }
        final long estimatedBytesBeforeAdd = bulkRequest.estimatedSizeInBytes() + calcEstimatedSizeInBytes(indexRequest);
        if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.numberOfActions() > 0) {
          flushBatch(bulkRequest);
          bulkRequest = bulkRequestSupplier.get();
        }
        bulkRequest.add(indexRequest);
      } catch (final IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    // Flush the remaining requests
    if (bulkRequest.numberOfActions() > 0) {
      flushBatch(bulkRequest);
    }
  }

  private long calcEstimatedSizeInBytes(final IndexRequest indexRequest) {
    // From BulkRequest#internalAdd(IndexRequest request)
    return (indexRequest.source() != null ? indexRequest.source().length() : 0) + REQUEST_OVERHEAD;
  }

  private void flushBatch(final BulkRequest bulkRequest) {
    bulkRequestTimer.record(() -> {
      try {
        bulkRetryStrategy.execute(bulkRequest);
      } catch (final InterruptedException e) {
        LOG.error("Unexpected Interrupt:", e);
        bulkRequestErrorsCounter.increment();
        Thread.currentThread().interrupt();
      }
    });
  }

  private void checkAndCreateIndexTemplate(final boolean isISMEnabled, final String ismPolicyId) throws IOException {
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final String indexTemplateName = indexAlias + "-index-template";

    // Check existing index template version - only overwrite if version is less than or does not exist
    if (!shouldCreateIndexTemplate(indexTemplateName)) {
      return;
    }

    final PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(indexTemplateName);
    final boolean isRaw = indexType.equals(IndexConstants.RAW);
    if (isRaw) {
      putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias + "-*"));
    } else {
      putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias));
    }
    if (isISMEnabled) {
      IndexStateManagement.attachPolicy(esSinkConfig.getIndexConfiguration(), ismPolicyId, indexAlias);
    }

    putIndexTemplateRequest.source(esSinkConfig.getIndexConfiguration().getIndexTemplate());
    restHighLevelClient.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
  }

  // TODO: Unit tests for this (and for the rest of the class)
  private boolean shouldCreateIndexTemplate(final String indexTemplateName) throws IOException {
    final Optional<IndexTemplateMetadata> indexTemplateMetadataOptional = getIndexTemplateMetadata(indexTemplateName);
    if (indexTemplateMetadataOptional.isPresent()) {
      final Integer existingTemplateVersion = indexTemplateMetadataOptional.get().version();
      LOG.info("Found version {} for existing index template {}", existingTemplateVersion, indexTemplateName);

      final int newTemplateVersion = (int) esSinkConfig.getIndexConfiguration().getIndexTemplate().getOrDefault("version", 0);

      if (existingTemplateVersion != null && existingTemplateVersion >= newTemplateVersion) {
        LOG.info("Index template {} should not be updated, current version {} >= existing version {}",
                indexTemplateName,
                existingTemplateVersion,
                newTemplateVersion);
        return false;

      } else {
        LOG.info("Index template {} should be updated from version {} to version {}",
                indexTemplateName,
                existingTemplateVersion,
                newTemplateVersion);
        return true;
      }
    } else {
      LOG.info("Index template {} does not exist and should be created", indexTemplateName);
      return true;
    }
  }

  private Optional<IndexTemplateMetadata> getIndexTemplateMetadata(final String indexTemplateName) throws IOException {
    final IndexTemplatesExistRequest existsRequest = new IndexTemplatesExistRequest(indexTemplateName);
    final boolean exists = restHighLevelClient.indices().existsTemplate(existsRequest, RequestOptions.DEFAULT);
    if (!exists) {
      return Optional.empty();
    }

    final GetIndexTemplatesRequest request = new GetIndexTemplatesRequest(indexTemplateName);
    final GetIndexTemplatesResponse response = restHighLevelClient.indices().getIndexTemplate(request, RequestOptions.DEFAULT);

    if (response.getIndexTemplates().size() == 1) {
      return Optional.of(response.getIndexTemplates().get(0));
    } else {
      throw new RuntimeException(String.format("Found multiple index templates (%s) result when querying for %s",
              response.getIndexTemplates().size(),
              indexTemplateName));
    }
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final boolean isRaw = indexType.equals(IndexConstants.RAW);
    final boolean exists = isRaw ?
            restHighLevelClient.indices().existsAlias(new GetAliasesRequest().aliases(indexAlias), RequestOptions.DEFAULT) :
            restHighLevelClient.indices().exists(new GetIndexRequest(indexAlias), RequestOptions.DEFAULT);
    if (!exists) {
      // TODO: use date as suffix?
      final String initialIndexName;
      final CreateIndexRequest createIndexRequest;
      if (isRaw) {
        initialIndexName = indexAlias + "-000001";
        createIndexRequest = new CreateIndexRequest(initialIndexName);
        createIndexRequest.alias(new Alias(indexAlias).writeIndex(true));
      } else {
        initialIndexName = indexAlias;
        createIndexRequest = new CreateIndexRequest(initialIndexName);
      }
      try {
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      } catch (OpenSearchException e) {
        if (e.getMessage().contains("resource_already_exists_exception")) {
          // Do nothing - likely caused by a race condition where the resource was created
          // by another host before this host's restClient made its request
        } else if (e.getMessage().contains(String.format(INDEX_ALIAS_USED_AS_INDEX_ERROR, indexAlias))) {
          // TODO: replace IOException with custom data-prepper exception
          throw new IOException(
                  String.format("An index exists with the same name as the reserved index alias name [%s], please delete or migrate the existing index",
                          indexAlias));
        } else {
          throw new IOException(e);
        }
      }
    }
  }

  private Map<String, Object> getMapFromJson(final String documentJson) throws IOException {
    final XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, documentJson);
    return parser.map();
  }

  private void logFailure(final DocWriteRequest<?> docWriteRequest, final Throwable failure) {
    if (dlqWriter != null) {
      try {
        dlqWriter.write(String.format("{\"Document\": [%s], \"failure\": %s}\n",
                docWriteRequest.toString(), failure.getMessage()));
      } catch (final IOException e) {
        LOG.error("DLQ failed for Document [{}]", docWriteRequest.toString());
      }
    } else {
      LOG.warn("Document [{}] has failure: {}", docWriteRequest.toString(), failure);
    }
  }

  @Override
  public void shutdown() {
    // Close the client
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
