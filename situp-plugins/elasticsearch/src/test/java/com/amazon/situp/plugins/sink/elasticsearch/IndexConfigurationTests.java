package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.CUSTOM;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.RAW;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.RAW_DEFAULT_TEMPLATE_FILE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.TYPE_TO_DEFAULT_ALIAS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IndexConfigurationTests {
  @Test
  public void testDefault() {
    final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();
    final URL expTemplateFile = indexConfiguration
        .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
    assertEquals(RAW, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateURL());
    assertEquals(5, indexConfiguration.getBulkSize());
  }

  @Test
  public void testRawAPMSpan() throws MalformedURLException {
    final String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());

    final String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
        .withIndexAlias(testIndexAlias).build();
    final URL expTemplateURL = indexConfiguration.getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testServiceMap() throws MalformedURLException {
    final String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
            .setIsServiceMap(true)
            .setIsRaw(false)
            .withTemplateFile(fakeTemplateFilePath)
            .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(SERVICE_MAP), indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());

    final String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
            .setIsServiceMap(true)
            .setIsRaw(false)
            .withIndexAlias(testIndexAlias)
            .build();
    final URL expTemplateURL = indexConfiguration
            .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testValidCustom() throws MalformedURLException {
    final String fakeTemplateFilePath = "src/resources/dummy.json";
    final String testIndexAlias = "foo";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
            .setIsRaw(false)
            .setIsServiceMap(false)
            .withIndexAlias(testIndexAlias)
            .withTemplateFile(fakeTemplateFilePath)
            .withBulkSize(10)
            .build();

    assertEquals(CUSTOM, indexConfiguration.getIndexType());
    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());
    assertEquals(10, indexConfiguration.getBulkSize());

    indexConfiguration = new IndexConfiguration.Builder()
            .setIsRaw(false)
            .setIsServiceMap(false)
            .withIndexAlias(testIndexAlias)
            .withBulkSize(-1)
            .build();
    assertEquals(-1, indexConfiguration.getBulkSize());
  }

  @Test
  public void testInvalidCustom() {
    // Missing index alias
    final IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder()
            .setIsRaw(false).setIsServiceMap(false);
    final Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
    assertEquals("Missing required properties:indexAlias", exception.getMessage());
  }

  @Test
  public void testReadIndexConfigDefault() {
    final PluginSetting pluginSetting = generatePluginSetting(
            null, null, null, null, null);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
    final URL expTemplateFile = indexConfiguration
            .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
    assertEquals(RAW, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateURL());
    assertEquals(5, indexConfiguration.getBulkSize());
  }

  @Test
  public void testReadIndexConfigInvalid() {
    final PluginSetting pluginSetting1 = generatePluginSetting(
            true, true, null, null, null);
    Exception e = assertThrows(IllegalStateException.class, () -> IndexConfiguration.readIndexConfig(pluginSetting1));
    assertTrue(e.getMessage().contains("trace_analytics_raw and trace_analytics_service_map cannot be both true."));

    final PluginSetting pluginSetting2 = generatePluginSetting(
            null, true, null, null, null);
    e = assertThrows(IllegalStateException.class, () -> IndexConfiguration.readIndexConfig(pluginSetting2));
    assertTrue(e.getMessage().contains("trace_analytics_raw and trace_analytics_service_map cannot be both true."));
  }

  @Test
  public void testReadIndexConfigCustom() throws MalformedURLException {
    final String fakeTemplateFilePath = "src/resources/dummy.json";
    final String testIndexAlias = "foo";
    final long testBulkSize = 10L;
    final PluginSetting pluginSetting = generatePluginSetting(
            false, false, testIndexAlias, fakeTemplateFilePath, testBulkSize);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
    assertEquals(CUSTOM, indexConfiguration.getIndexType());
    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());
    assertEquals(testBulkSize, indexConfiguration.getBulkSize());
  }

  private PluginSetting generatePluginSetting(final Boolean isRaw, final Boolean isServiceMap, final String indexAlias,
                                              final String templateFilePath, final Long bulkSize) {
    final Map<String, Object> metadata = new HashMap<>();
    if (isRaw != null) {
      metadata.put(IndexConfiguration.TRACE_ANALYTICS_RAW_FLAG, isRaw);
    }
    if (isServiceMap != null) {
      metadata.put(IndexConfiguration.TRACE_ANALYTICS_SERVICE_MAP_FLAG, isServiceMap);
    }
    if (indexAlias != null) {
      metadata.put("index_alias", indexAlias);
    }
    if (templateFilePath != null) {
      metadata.put("template_file", templateFilePath);
    }
    if (bulkSize != null) {
      metadata.put("bulk_size", bulkSize);
    }

    return new PluginSetting("elasticsearch", metadata);
  }
}
