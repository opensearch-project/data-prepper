package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.sink.Sink;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.entity.BufferedHttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.CONNECT_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.HOSTS;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.PASSWORD;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.SOCKET_TIMEOUT;
import static com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration.USERNAME;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_ALIAS;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.INDEX_TYPE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration.TEMPLATE_FILE;

@SitupPlugin(name = "amazon_es", type = PluginType.SINK)
public class ElasticsearchSink implements Sink<Record<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

  private final ElasticsearchSinkConfiguration esSinkConfig;
  private RestClient restClient;

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
    return builder.build();
  }

  public void start() throws IOException {
    restClient = esSinkConfig.getConnectionConfiguration().createClient();
    if (esSinkConfig.getIndexConfiguration().getTemplateURL() != null) {
      createIndexTemplate();
    }
    checkAndCreateIndex();
  }

  @Override
  public boolean output(Collection<Record<String>> records) {
    if (records.isEmpty()) {
      return false;
    }
    StringBuilder bulkRequest = new StringBuilder();
    for (final Record<String> record: records) {
      String document = record.getData();
      try {
        XContentBuilder xcontentBuilder = XContentFactory.jsonBuilder();
        xcontentBuilder.startObject().startObject("index");
        String spanId = getSpanIdFromRecord(document);
        if (spanId != null) {
          xcontentBuilder.field("_id", spanId);
        }
        xcontentBuilder.endObject().endObject();
        bulkRequest.append(Strings.toString(xcontentBuilder)).append("\n").append(document).append("\n");
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    Response response;
    HttpEntity responseEntity;
    final String endPoint = String.format("/%s/_bulk", esSinkConfig.getIndexConfiguration().getIndexAlias());
    final Request request = new Request(HttpMethod.POST, endPoint);
    request.setJsonEntity(bulkRequest.toString());
    try {
      response = restClient.performRequest(request);
      responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry(HttpMethod.POST, endPoint, responseEntity);
      checkForErrors(responseEntity);

      // TODO: what if partial success?
      return true;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  // TODO: need to be invoked by pipeline
  public void stop() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  private void createIndexTemplate() throws IOException {
    // TODO: add logic here to create index template
    // QUES: how to identify index template file with index pattern accordingly?
    Response response;
    HttpEntity responseEntity;
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    final String endPoint = String.format("/_template/%s-index-template", indexAlias);
    final URL jsonURL = esSinkConfig.getIndexConfiguration().getTemplateURL();
    final String templateJson = readTemplateURL(jsonURL);
    final Request request = new Request(HttpMethod.POST, endPoint);
    final XContentParser parser = XContentFactory.xContent(XContentType.JSON)
        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, templateJson);
    Map<String, Object> template = parser.map();
    String jsonEntity;
    if (esSinkConfig.getIndexConfiguration().getIndexType().equals(IndexConstants.RAW)) {
      // Add -* prefix for rollover
      jsonEntity = Strings.toString(
          XContentFactory.jsonBuilder().startObject()
              .field("index_patterns", indexAlias + "-*")
              .field("settings", template.getOrDefault("settings", new HashMap<>()))
              .field("mappings", template.getOrDefault("mappings", new HashMap<>())).endObject());
    } else {
      jsonEntity = Strings.toString(
          XContentFactory.jsonBuilder().startObject()
              .field("index_patterns", indexAlias)
              .field("settings", template.getOrDefault("settings", new HashMap<>()))
              .field("mappings", template.getOrDefault("mappings", new HashMap<>())).endObject());
    }
    request.setJsonEntity(jsonEntity);
    response = restClient.performRequest(request);
    responseEntity = new BufferedHttpEntity(response.getEntity());
    // TODO: apply retry predicate here
    responseEntity = handleRetry(HttpMethod.POST, endPoint, responseEntity);
    checkForErrors(responseEntity);
  }

  private void checkAndCreateIndex() throws IOException {
    // Check alias exists
    final String indexAlias = esSinkConfig.getIndexConfiguration().getIndexAlias();
    Request request = new Request(HttpMethod.HEAD, "/" + indexAlias);
    Response response = restClient.performRequest(request);
    final StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
      // TODO: use date as suffix?
      String initialIndexName;
      if (esSinkConfig.getIndexConfiguration().getIndexType().equals(IndexConstants.RAW)) {
        initialIndexName = indexAlias + "-000001";
        request = new Request(HttpMethod.PUT, "/" + initialIndexName);
        String jsonContent = Strings.toString(
            XContentFactory.jsonBuilder().startObject()
                .startObject("aliases")
                .startObject(indexAlias)
                .field("is_write_index", true)
                .endObject()
                .endObject()
                .endObject()
        );
        request.setJsonEntity(jsonContent);
      } else {
        initialIndexName = indexAlias;
        request = new Request(HttpMethod.PUT, "/" + initialIndexName);
      }
      response = restClient.performRequest(request);
      HttpEntity responseEntity = new BufferedHttpEntity(response.getEntity());
      // TODO: apply retry predicate here
      responseEntity = handleRetry(HttpMethod.POST, initialIndexName, responseEntity);
      checkForErrors(responseEntity);
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

  private String getSpanIdFromRecord(String documentJson) throws IOException {
    final XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, documentJson);
    return (String)parser.map().get("spanId");
  }

  private HttpEntity handleRetry(String method, String endpoint, HttpEntity requestBody) {
    // TODO: add logic here
    return null;
  }

  private void checkForErrors(HttpEntity responseEntity) {
    // TODO: add logic to find errors in the response entity.
  }
}
