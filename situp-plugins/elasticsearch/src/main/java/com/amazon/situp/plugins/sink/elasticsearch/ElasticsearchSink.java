package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@SitupPlugin(name = "elasticsearch", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);
  // Pulled from BulkRequest to make estimation of bytes consistent
  private static final int REQUEST_OVERHEAD = 50;

  private BufferedWriter dlqWriter;
  private final ElasticsearchSinkConfiguration esSinkConfig;
  private RestHighLevelClient restHighLevelClient;
  private Supplier<BulkRequest> bulkRequestSupplier;
  private BulkRetryStrategy bulkRetryStrategy;
  private final long bulkSize;
  private final String indexType;
  private final String documentIdField;

  public ElasticsearchSink(final PluginSetting pluginSetting) {
    this.esSinkConfig = ElasticsearchSinkConfiguration.readESConfig(pluginSetting);
    this.bulkSize = ByteSizeUnit.MB.toBytes(esSinkConfig.getIndexConfiguration().getBulkSize());
    this.indexType = esSinkConfig.getIndexConfiguration().getIndexType();
    this.documentIdField = esSinkConfig.getIndexConfiguration().getDocumentIdField();
    try {
      start();
    } catch (final IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public void start() throws IOException {
    restHighLevelClient = esSinkConfig.getConnectionConfiguration().createClient();
    final String ismPolicyId = IndexStateManagement.checkAndCreatePolicy(restHighLevelClient, indexType);
    if (esSinkConfig.getIndexConfiguration().getTemplateURL() != null) {
      createIndexTemplate(ismPolicyId);
    }
    final String dlqFile = esSinkConfig.getRetryConfiguration().getDlqFile();
    if ( dlqFile != null) {
      dlqWriter = Files.newBufferedWriter(Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    checkAndCreateIndex();
    bulkRequestSupplier = () -> new BulkRequest(esSinkConfig.getIndexConfiguration().getIndexAlias());
    bulkRetryStrategy = new BulkRetryStrategy(
            bulkRequest -> restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT),
            this::logFailure,
            bulkRequestSupplier);
  }

  @Override
  public void output(final Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return;
    }
    BulkRequest bulkRequest = bulkRequestSupplier.get();
    for (final Record<String> record: records) {
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
    try {
      bulkRetryStrategy.execute(bulkRequest);
    } catch (final InterruptedException e) {
      LOG.error("Unexpected Interrupt:", e);
      Thread.currentThread().interrupt();
    }
  }

  // TODO: need to be invoked by pipeline
  public void stop() {
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

  @SuppressWarnings("unchecked")
  private void createIndexTemplate(final String ismPolicyId) throws IOException {
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(indexAlias + "-index-template");
    final boolean isRaw = indexType.equals(IndexConstants.RAW);
    if (isRaw) {
      putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias + "-*"));
    } else {
      putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias));
    }
    final URL jsonURL = esSinkConfig.getIndexConfiguration().getTemplateURL();
    final Map<String, Object> template = readTemplateURL(jsonURL);
    final Map<String, Object> settings = (Map<String, Object>) template.getOrDefault("settings", new HashMap<>());
    if (ismPolicyId != null) {
      IndexStateManagement.attachPolicy(settings, ismPolicyId, indexAlias);
    }
    putIndexTemplateRequest.source(template);
    restHighLevelClient.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final boolean isRaw = indexType.equals(IndexConstants.RAW);
    final boolean exists = isRaw?
            restHighLevelClient.indices().existsAlias(new GetAliasesRequest().aliases(indexAlias), RequestOptions.DEFAULT):
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
      restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }
  }

  private Map<String, Object> readTemplateURL(final URL templateURL) throws IOException {
    return new ObjectMapper().readValue(templateURL, new TypeReference<Map<String, Object>>() {});
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
    };
  }
}
