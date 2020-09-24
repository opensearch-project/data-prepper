package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
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
  private static final String DEFAULT_SERVICE_MAP_FILE = "service-map-1.json";

  public void testInstantiateSinkRawSpanDefault() throws IOException {
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
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
    final String rolloverIndexName = String.format("%s-000002", indexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testOutputRawSpanDefault() throws IOException, InterruptedException {
    final String testDoc = readDocFromFile(DEFAULT_RAW_SPAN_FILE);
    final ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked") final Map<String, Object> expData = mapper.readValue(testDoc, Map.class);

    final List<Record<String>> testRecords = Collections.singletonList(new Record<>(testDoc));
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final boolean success = sink.output(testRecords);
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    assertTrue(success);
    final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));
    assertEquals(Integer.valueOf(1), getDocumentCount(expIndexAlias, "_id", (String)expData.get("spanId")));
    sink.stop();
  }

  public void testOutputRawSpanWithDLQ() throws IOException, InterruptedException {
    // TODO: write test case
    final String testDoc1 = readDocFromFile("raw-span-error.json");
    final String testDoc2 = readDocFromFile(DEFAULT_RAW_SPAN_FILE);
    final ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked") final Map<String, Object> expData = mapper.readValue(testDoc2, Map.class);
    final List<Record<String>> testRecords = Arrays.asList(new Record<>(testDoc1), new Record<>(testDoc2));
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, null, null);
    // generate temporary directory for dlq file
    final File tempDirectory = Files.createTempDirectory("").toFile();
    // add dlq file path into setting
    final String expDLQFile = tempDirectory.getAbsolutePath() + "/test-dlq.txt";
    pluginSetting.getSettings().put(RetryConfiguration.DLQ_FILE, expDLQFile);

    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final boolean success = sink.output(testRecords);
    sink.stop();
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    assertTrue(success);
    final StringBuilder content = new StringBuilder();
    Files.lines(Paths.get(expDLQFile)).forEach(content::append);
    assertTrue(content.toString().contains(testDoc1));
    final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);
    final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));

    // clean up temporary directory
    assertTrue(deleteDirectory(tempDirectory));
  }

  public void testInstantiateSinkRawSpanCustom() throws IOException {
    final String testIndexAlias = "test-raw-span";
    final String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, testIndexAlias, testTemplateFile);
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
    final String rolloverIndexName = String.format("%s-000002", testIndexAlias);
    request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
    response = client().performRequest(request);
    assertEquals(true, checkIsWriteIndex(EntityUtils.toString(response.getEntity()), testIndexAlias, rolloverIndexName));
    sink.stop();
  }

  public void testOutputRawSpanCustom() throws IOException, InterruptedException {
    final String testIndexAlias = "test-raw-span";
    final String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    final String traceId = UUID.randomUUID().toString();
    final String spanId1 = UUID.randomUUID().toString();
    final String spanId2 = UUID.randomUUID().toString();
    final List<Record<String>> testRecords = Arrays.asList(
        generateDummyRawSpanRecord(traceId, spanId1, "2020-08-05", "2020-08-06"),
        generateDummyRawSpanRecord(traceId, spanId2, "2020-08-30", "2020-09-01")
    );
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.RAW, testIndexAlias, testTemplateFile);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final boolean success = sink.output(testRecords);
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    assertTrue(success);
    assertEquals(Integer.valueOf(2), getDocumentCount(testIndexAlias, "traceId", traceId));
    // startTime field should no longer be detected as datetime according to test-index-template.json
    assertEquals(Integer.valueOf(0), getDocumentCount(testIndexAlias, "startTime", "2020-08-05T00:00:00.000Z"));
    assertEquals(Integer.valueOf(1), getDocumentCount(testIndexAlias, "endTime", "2020-09-01"));
    sink.stop();
  }

  public void testInstantiateSinkServiceMapDefault() throws IOException {
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, null, null);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.SERVICE_MAP);
    final Request request = new Request(HttpMethod.HEAD, indexAlias);
    final Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testOutputServiceMapDefault() throws IOException, InterruptedException {
    final String testDoc = readDocFromFile(DEFAULT_SERVICE_MAP_FILE);
    final ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    final Map<String, Object> expData = mapper.readValue(testDoc, Map.class);

    final List<Record<String>> testRecords = Collections.singletonList(new Record<>(testDoc));
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, null, null);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final boolean success = sink.output(testRecords);
    // wait for documents to be populated
    // TODO: better wait strategy?
    Thread.sleep(1000);

    final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.SERVICE_MAP);
    assertTrue(success);
    final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
    assertEquals(1, retSources.size());
    assertEquals(expData, retSources.get(0));
    assertEquals(Integer.valueOf(1), getDocumentCount(expIndexAlias, "_id", (String)expData.get("hashId")));
    sink.stop();
  }

  public void testInstantiateSinkServiceMapCustom() throws IOException {
    final String testIndexAlias = "test-service-map";
    final String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.SERVICE_MAP, testIndexAlias, testTemplateFile);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    final Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  public void testInstantiateSinkCustomIndex() throws IOException {
    final String testIndexAlias = "test-alias";
    final String testTemplateFile = Objects.requireNonNull(
            getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
    final PluginSetting pluginSetting = generatePluginSetting(IndexConstants.CUSTOM, testIndexAlias, testTemplateFile);
    final ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
    final Request request = new Request(HttpMethod.HEAD, testIndexAlias);
    final Response response = client().performRequest(request);
    assertEquals(SC_OK, response.getStatusLine().getStatusCode());
    sink.stop();
  }

  private PluginSetting generatePluginSetting(final String indexType, final String indexAlias, final String templateFilePath) {
    final Map<String, Object> metadata = new HashMap<>();
    metadata.put("index_type", indexType);
    metadata.put("hosts", HOSTS);
    metadata.put("index_alias", indexAlias);
    metadata.put("template_file", templateFilePath);

    return new PluginSetting("elasticsearch", metadata);
  }

  private Record<String> generateDummyRawSpanRecord(final String traceId, final String spanId, final String startTime, final String endTime) throws IOException {
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

  private String readDocFromFile(final String filename) throws IOException {
    final StringBuilder jsonBuilder = new StringBuilder();
    try (final InputStream inputStream = Objects.requireNonNull(
            getClass().getClassLoader().getResourceAsStream(filename))){
      final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      bufferedReader.lines().forEach(jsonBuilder::append);
    }
    return jsonBuilder.toString();
  }

  private Boolean checkIsWriteIndex(final String responseBody, final String aliasName, final String indexName) throws IOException {
    @SuppressWarnings("unchecked") final Map<String, Object> indexBlob = (Map<String, Object>)createParser(XContentType.JSON.xContent(), responseBody).map().get(indexName);
    @SuppressWarnings("unchecked") final Map<String, Object> aliasesBlob = (Map<String, Object>)indexBlob.get("aliases");
    @SuppressWarnings("unchecked") final Map<String, Object> aliasBlob = (Map<String, Object>)aliasesBlob.get(aliasName);
    return (Boolean) aliasBlob.get("is_write_index");
  }

  private Integer getDocumentCount(final String index, final String field, final String value) throws IOException, InterruptedException {
    final Request request = new Request(HttpMethod.GET, index + "/_count");
    if (field != null && value != null) {
      final String jsonEntity = Strings.toString(
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
    final Response response = client().performRequest(request);
    final String responseBody = EntityUtils.toString(response.getEntity());
    return (Integer)createParser(XContentType.JSON.xContent(), responseBody).map().get("count");
  }

  private List<Map<String, Object>> getSearchResponseDocSources(final String index) throws IOException {
    final Request request = new Request(HttpMethod.GET, index + "/_search");
    final Response response = client().performRequest(request);
    final String responseBody = EntityUtils.toString(response.getEntity());

    @SuppressWarnings("unchecked") final List<Object> hits = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
            responseBody).map().get("hits")).get("hits");
    @SuppressWarnings("unchecked") final List<Map<String, Object>> sources = hits.stream()
            .map(hit -> (Map<String, Object>)((Map<String, Object>) hit).get("_source"))
            .collect(Collectors.toList());
    return sources;
  }

  private boolean deleteDirectory(final File directoryToBeDeleted) {
    final File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (final File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }
}
