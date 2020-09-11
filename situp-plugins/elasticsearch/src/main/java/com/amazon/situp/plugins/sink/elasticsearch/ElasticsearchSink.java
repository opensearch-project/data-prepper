package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.CONNECT_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.HOSTS;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.PASSWORD;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.SOCKET_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.USERNAME;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.BATCH_SIZE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_ALIAS;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_TYPE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.TEMPLATE_FILE;

@SitupPlugin(name = "elasticsearch", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

  private final ElasticsearchSinkConfiguration esSinkConfig;
  private RestHighLevelClient restHighLevelClient;
  private BulkProcessor bulkProcessor;

  public ElasticsearchSink(final PluginSetting pluginSetting) {
    this.esSinkConfig = readESConfig(pluginSetting);
    try {
      start();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private ElasticsearchSinkConfiguration readESConfig(final PluginSetting pluginSetting) {
    final ConnectionConfiguration connectionConfiguration = readConnectionConfiguration(pluginSetting);
    final IndexConfiguration indexConfiguration = readIndexConfig(pluginSetting);

    return new ElasticsearchSinkConfiguration.Builder(connectionConfiguration)
            .withIndexConfiguration(indexConfiguration)
            .build();
  }

  private ConnectionConfiguration readConnectionConfiguration(final PluginSetting pluginSetting){
    @SuppressWarnings("unchecked")
    final List<String> hosts = (List<String>)pluginSetting.getAttributeFromSettings(HOSTS);
    ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(hosts);
    final String username = (String)pluginSetting.getAttributeFromSettings(USERNAME);
    if (username != null) {
      builder = builder.withUsername(username);
    }
    final String password = (String)pluginSetting.getAttributeFromSettings(PASSWORD);
    if (password != null) {
      builder = builder.withPassword(password);
    }
    final Integer socketTimeout = (Integer)pluginSetting.getAttributeFromSettings(SOCKET_TIMEOUT);
    if (socketTimeout != null) {
      builder = builder.withSocketTimeout(socketTimeout);
    }
    final Integer connectTimeout = (Integer)pluginSetting.getAttributeFromSettings(CONNECT_TIMEOUT);
    if (connectTimeout != null) {
      builder = builder.withConnectTimeout(connectTimeout);
    }

    return builder.build();
  }

  private IndexConfiguration readIndexConfig(final PluginSetting pluginSetting) {
    IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
    final String indexType = (String)pluginSetting.getAttributeFromSettings(INDEX_TYPE);
    if (indexType != null) {
      builder = builder.withIndexType(indexType);
    }
    final String indexAlias = (String)pluginSetting.getAttributeFromSettings(INDEX_ALIAS);
    if (indexAlias != null) {
      builder = builder.withIndexAlias(indexAlias);
    }
    final String templateFile = (String)pluginSetting.getAttributeFromSettings(TEMPLATE_FILE);
    if (templateFile != null) {
      builder = builder.withTemplateFile(templateFile);
    }
    final Long batchSize = (Long)pluginSetting.getAttributeFromSettings(BATCH_SIZE);
    if (batchSize != null) {
      builder = builder.withBatchSize(batchSize);
    }
    return builder.build();
  }

  public void start() throws IOException {
    restHighLevelClient = esSinkConfig.getConnectionConfiguration().createClient();
    if (esSinkConfig.getIndexConfiguration().getTemplateURL() != null) {
      createIndexTemplate();
    }
    checkAndCreateIndex();
    bulkProcessor = initBulkProcessor(restHighLevelClient);
  }

  private BulkProcessor initBulkProcessor(RestHighLevelClient client) {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {

      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            BulkResponse response) {
        if (response.hasFailures()) {
          for (BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
              BulkItemResponse.Failure failure =
                      bulkItemResponse.getFailure();
              LOG.warn("Document [{}] has failure: {}", bulkItemResponse.getId(), failure);
            }
          }
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            Throwable failure) {
        LOG.error(failure.getMessage(), failure);
      }
    };

    // TODO: customize retry backoff, concurrency settings, etc.
    return BulkProcessor.builder(
            (request, bulkListener) -> client.bulkAsync(
                    request, RequestOptions.DEFAULT, bulkListener), listener)
            .setGlobalIndex(esSinkConfig.getIndexConfiguration().getIndexAlias())
            .setBulkSize(new ByteSizeValue(esSinkConfig.getIndexConfiguration().getBatchSize(), ByteSizeUnit.BYTES))
            .build();
  }

  @Override
  public boolean output(Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return false;
    }
    for (final Record<String> record: records) {
      String document = record.getData();
      IndexRequest indexRequest = new IndexRequest().source(document, XContentType.JSON);
      try {
        Map<String, Object> docMap = getMapFromJson(document);
        String spanId = (String)docMap.get("spanId");
        if (spanId != null) {
          indexRequest = indexRequest.id(spanId);
        }
        bulkProcessor.add(indexRequest);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    return true;
  }

  // TODO: need to be invoked by pipeline
  public void stop() {
    try {
      boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
      if (!terminated) {
        LOG.warn("Timed out for flushing remaining requests");
      }
      // client needs to be closed separately
      restHighLevelClient.close();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
      bulkProcessor.close();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void createIndexTemplate() throws IOException {
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(indexAlias + "-index-template");
    putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias + "-*"));
    final URL jsonURL = esSinkConfig.getIndexConfiguration().getTemplateURL();
    final String templateJson = readTemplateURL(jsonURL);
    putIndexTemplateRequest.source(templateJson, XContentType.JSON);
    restHighLevelClient.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases(indexAlias);
    boolean exists = restHighLevelClient.indices().existsAlias(getAliasesRequest, RequestOptions.DEFAULT);
    if (!exists) {
      // TODO: use date as suffix?
      String initialIndexName;
      CreateIndexRequest createIndexRequest;
      if (esSinkConfig.getIndexConfiguration().getIndexType().equals(IndexConstants.RAW)) {
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

  private String readTemplateURL(final URL templateURL) throws IOException {
    final StringBuilder templateJsonBuffer = new StringBuilder();
    InputStream is = templateURL.openStream();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
      reader.lines().forEach(line -> templateJsonBuffer.append(line).append("\n"));
    }
    is.close();
    return templateJsonBuffer.toString();
  }

  private Map<String, Object> getMapFromJson(String documentJson) throws IOException {
    final XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, documentJson);
    return parser.map();
  }
}
