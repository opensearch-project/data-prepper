package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
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
  private static final String DEFAULT_RAW_SPAN_FILE = "raw-span-1.json";

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
    String testDoc = readDocFromFile(DEFAULT_RAW_SPAN_FILE);
    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, Object> expData = mapper.readValue(testDoc, Map.class);

    List<Record<String>> testRecords = Collections.singletonList(new Record<>(testDoc));
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    boolean success = sink.output(testRecords);
    sink.stop();
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    assertTrue(success);
    List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));
  }

  public void testOutputRawSpanWithDLQ() throws IOException, InterruptedException {
    // TODO: write test case
    String testDoc1 = readDocFromFile("raw-span-error.json");
    String testDoc2 = readDocFromFile(DEFAULT_RAW_SPAN_FILE);
    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, Object> expData = mapper.readValue(testDoc2, Map.class);
    List<Record<String>> testRecords = Arrays.asList(new Record<>(testDoc1), new Record<>(testDoc2));
    PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    // generate temporary directory for dlq file
    File tempDirectory = Files.createTempDirectory("").toFile();
    // add dlq file path into setting
    String expDLQFile = tempDirectory.getAbsolutePath() + "/test-dlq.txt";
    pluginSetting.getSettings().put(RetryConfiguration.DLQ_FILE, expDLQFile);

    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    boolean success = sink.output(testRecords);
    sink.stop();
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    assertTrue(success);
    StringBuilder content = new StringBuilder();
    Files.lines(Paths.get(expDLQFile)).forEach(content::append);
    assertTrue(content.toString().contains(testDoc1));
    String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));

    // clean up temporary directory
    assertTrue(deleteDirectory(tempDirectory));
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
    // TODO: better wait strategy?
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

  private String readDocFromFile(String filename) throws IOException {
    final StringBuilder jsonBuilder = new StringBuilder();
    try (InputStream inputStream = Objects.requireNonNull(
            getClass().getClassLoader().getResourceAsStream(filename))){
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      bufferedReader.lines().forEach(jsonBuilder::append);
    }
    return jsonBuilder.toString();
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

  private boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }
}
