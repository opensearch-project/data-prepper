package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.http.HttpStatus.SC_OK;

public class ElasticsearchSinkIT extends ESRestTestCase {
  public static List<String> HOSTS = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
      .map(ip -> "http://" + ip).collect(Collectors.toList());
  private static final String DEFAULT_TEMPLATE_FILE = "test-index-template.json";

  public void testInstantiateSinkRawSpanDefault() throws IOException {
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();

    // roll over initial index
    request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
    request.setJsonEntity("{ \"conditions\" : { } }\n");
    response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());

    // Instantiate sink again
    sink = new ElasticsearchSink(pluginSetting);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", indexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testOutputRawSpanDefault() throws IOException, InterruptedException {
    String testDoc = "{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\"," +
            "\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"kind\":\"SERVER\",\"status\":{}," +
            "\"startTime\":\"2020-08-20T05:40:46.041011600Z\",\"endTime\":\"2020-08-20T05:40:46.089556800Z\"," +
            "\"attributes.http.status_code\":200,\"attributes.net.peer.port\":41168," +
            "\"attributes.servlet.path\":\"/logs\",\"attributes.http.response_content_length\":7," +
            "\"attributes.http.user_agent\":\"curl/7.54.0\",\"attributes.http.flavor\":\"HTTP/1.1\"," +
            "\"attributes.servlet.context\":\"\",\"attributes.http.url\":\"http://0.0.0.0:8087/logs\"," +
            "\"attributes.net.peer.ip\":\"172.29.0.1\",\"attributes.http.method\":\"POST\"," +
            "\"attributes.http.client_ip\":\"172.29.0.1\"," +
            "\"resource.attributes.service.name\":\"analytics-service\"," +
            "\"resource.attributes.telemetry.sdk.language\":\"java\"," +
            "\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\"," +
            "\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}";

    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, Object> expData = mapper.readValue(testDoc, Map.class);

    List<Record<String>> testRecords = Collections.singletonList(new Record<>(testDoc));
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    boolean success = sink.output(testRecords);
    sink.stop();
    // wait for documents to be populated
    Thread.sleep(1000);

    String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    assertTrue(success);
    List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));
  }

  public void testInstantiateSinkRawSpanCustom() throws IOException {
    String testIndexAlias = "test-raw-span";
    String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();

    // roll over initial index
    request = new Request(HttpMethod.POST, String.format("%s/_rollover", testIndexAlias));
    request.setJsonEntity("{ \"conditions\" : { } }\n");
    response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());

    // Instantiate sink again
    sink = new ElasticsearchSink(pluginSetting);
    // Make sure no new write index *-000001 is created under alias
    String rolloverIndexName = String.format("%s-000002", testIndexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), testIndexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testOutputRawSpanCustom() throws IOException, InterruptedException {
    String testIndexAlias = "test-raw-span";
    String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    String traceId = UUID.randomUUID().toString();
    String spanId1 = UUID.randomUUID().toString();
    String spanId2 = UUID.randomUUID().toString();
    List<Record<String>> testRecords = Arrays.asList(
        generateDummyRawSpanRecord(traceId, spanId1, "2020-08-05", "2020-08-06"),
        generateDummyRawSpanRecord(traceId, spanId2, "2020-08-30", "2020-09-01")
    );
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    boolean success = sink.output(testRecords);
    sink.stop();
    // wait for documents to be populated
    Thread.sleep(1000);

    assertTrue(success);
    assertEquals(Integer.valueOf(2), getDocumentCount(testIndexAlias, "traceId", traceId));
    // startTime field should no longer be detected as datetime according to test-index-template.json
    assertEquals(Integer.valueOf(0), getDocumentCount(testIndexAlias, "startTime", "2020-08-05T00:00:00.000Z"));
    assertEquals(Integer.valueOf(1), getDocumentCount(testIndexAlias, "endTime", "2020-09-01"));
  }

  public void testInstantiateSinkServiceMapDefault() throws IOException {
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.SERVICE_MAP);
    Request request = new Request(HttpMethod.HEAD, indexAlias);
    Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testInstantiateSinkServiceMapCustom() throws IOException {
    String testIndexAlias = "test-service-map";
    String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testInstantiateSinkCustomIndex() throws IOException {
    String testIndexAlias = "test-alias";
    String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.CUSTOM, testIndexAlias, testTemplateFile);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  private PluginSetting generatePluginSetting(String indexType, String indexAlias, String templateFilePath) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("index_type", indexType);
    metadata.put("hosts", HOSTS);
    metadata.put("index_alias", indexAlias);
    metadata.put("template_file", templateFilePath);

    return new PluginSetting("elasticsearch", metadata);
  }

  private Record<String> generateDummyRawSpanRecord(String traceId, String spanId, String startTime, String endTime) throws IOException {
    return new Record<>(
        Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("traceId", traceId)
                .field("spanId", spanId)
                .field("startTime", startTime)
                .field("endTime", endTime)
                .endObject()
        )
    );
  }

  private Boolean checkIsWriteIndex(String responseBody, String aliasName, String indexName) throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, Object> indexBlob = (Map<String, Object>)createParser(XContentType.JSON.xContent(), responseBody).map().get(indexName);
    @SuppressWarnings("unchecked")
    Map<String, Object> aliasesBlob = (Map<String, Object>)indexBlob.get("aliases");
    @SuppressWarnings("unchecked")
    Map<String, Object> aliasBlob = (Map<String, Object>)aliasesBlob.get(aliasName);
    return (Boolean) aliasBlob.get("is_write_index");
  }

  private Integer getDocumentCount(String index, String field, String value) throws IOException, InterruptedException {
    Request request = new Request(HttpMethod.GET, index + "/_count");
    if (field != null && value != null) {
      String jsonEntity = Strings.toString(
          XContentFactory.jsonBuilder().startObject()
              .startObject("query")
              .startObject("match")
              .field(field, value)
              .endObject()
              .endObject()
              .endObject()
      );
      request.setJsonEntity(jsonEntity);
    }
    Response response = client().performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    return (Integer)createParser(XContentType.JSON.xContent(), responseBody).map().get("count");
  }

  private List<Map<String, Object>> getSearchResponseDocSources(String index) throws IOException {
    Request request = new Request(HttpMethod.GET, index + "/_search");
    Response response = client().performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());

    @SuppressWarnings("unchecked")
    List<Object> hits = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
            responseBody).map().get("hits")).get("hits");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> sources = hits.stream()
            .map(hit -> (Map<String, Object>)((Map<String, Object>) hit).get("_source"))
            .collect(Collectors.toList());
    return sources;
  }
}
