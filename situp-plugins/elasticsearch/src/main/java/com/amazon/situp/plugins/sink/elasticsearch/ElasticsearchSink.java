package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;

import org.elasticsearch.common.unit.ByteSizeUnit;
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
import java.util.function.Supplier;

import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.CONNECT_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.HOSTS;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.PASSWORD;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.SOCKET_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.USERNAME;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.BULK_SIZE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_ALIAS;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_TYPE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.TEMPLATE_FILE;

@SitupPlugin(name = "elasticsearch", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

  // Pulled from BulkRequest to make estimation of bytes consistent
  private static final int REQUEST_OVERHEAD = 50;

  private final ElasticsearchSinkConfiguration esSinkConfig;
  private RestHighLevelClient restHighLevelClient;
  private Supplier<BulkRequest> bulkRequestSupplier;
  private final long bulkSize;

  public ElasticsearchSink(final PluginSetting pluginSetting) {
    this.esSinkConfig = readESConfig(pluginSetting);
    this.bulkSize = ByteSizeUnit.MB.toBytes(esSinkConfig.getIndexConfiguration().getBulkSize());
    try {
      start();
    } catch (final IOException e) {
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
    final Long batchSize = (Long)pluginSetting.getAttributeFromSettings(BULK_SIZE);
    if (batchSize != null) {
      builder = builder.withBulkSize(batchSize);
    }
    return builder.build();
  }

  public void start() throws IOException {
    restHighLevelClient = esSinkConfig.getConnectionConfiguration().createClient();
    if (esSinkConfig.getIndexConfiguration().getTemplateURL() != null) {
      createIndexTemplate();
    }
    checkAndCreateIndex();
    bulkRequestSupplier = () -> new BulkRequest(esSinkConfig.getIndexConfiguration().getIndexAlias());
  }

  @Override
  public boolean output(final Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return false;
    }
    boolean success = true;
    BulkRequest bulkRequest = bulkRequestSupplier.get();
    for (final Record<String> record: records) {
      final String document = record.getData();
      final IndexRequest indexRequest = new IndexRequest().source(document, XContentType.JSON);
      try {
        final Map<String, Object> docMap = getMapFromJson(document);
        final String spanId = (String)docMap.get("spanId");
        if (spanId != null) {
          indexRequest.id(spanId);
        }
        final long estimatedBytesBeforeAdd = bulkRequest.estimatedSizeInBytes() + calcEstimatedSizeInBytes(indexRequest);
        if (bulkSize >= 0 && estimatedBytesBeforeAdd >= bulkSize && bulkRequest.numberOfActions() > 0) {
          success = success && flushBatch(bulkRequest);
          bulkRequest = bulkRequestSupplier.get();
        }
        bulkRequest.add(indexRequest);
      } catch (final IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    // Flush the remaining requests
    if (bulkRequest.numberOfActions() > 0) {
      success = success && flushBatch(bulkRequest);
    }

    return success;
  }

  private long calcEstimatedSizeInBytes(final IndexRequest indexRequest) {
    // From BulkRequest#internalAdd(IndexRequest request)
    return (indexRequest.source() != null ? indexRequest.source().length() : 0) + REQUEST_OVERHEAD;
  }

  private boolean flushBatch(final BulkRequest bulkRequest) {
    try {
      final BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
      // TODO: apply retry here
      // TODO: what if partial success?
      return !bulkResponse.hasFailures();
    } catch (final IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
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
  }

  private void createIndexTemplate() throws IOException {
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(indexAlias + "-index-template");
    putIndexTemplateRequest.patterns(Collections.singletonList(indexAlias + "-*"));
    final URL jsonURL = esSinkConfig.getIndexConfiguration().getTemplateURL();
    final String templateJson = readTemplateURL(jsonURL);
    putIndexTemplateRequest.source(templateJson, XContentType.JSON);
    restHighLevelClient.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases(indexAlias);
    final boolean exists = restHighLevelClient.indices().existsAlias(getAliasesRequest, RequestOptions.DEFAULT);
    if (!exists) {
      // TODO: use date as suffix?
      final String initialIndexName;
      final CreateIndexRequest createIndexRequest;
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
    final InputStream is = templateURL.openStream();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
      reader.lines().forEach(line -> templateJsonBuffer.append(line).append("\n"));
    }
    is.close();
    return templateJsonBuffer.toString();
  }

  private Map<String, Object> getMapFromJson(final String documentJson) throws IOException {
    final XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, documentJson);
    return parser.map();
  }
}
