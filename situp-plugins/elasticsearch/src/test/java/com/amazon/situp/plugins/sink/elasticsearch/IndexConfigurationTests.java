package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
  public void testRawAPMSpan() {
    final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().setIsRaw(true).build();
    final URL expTemplateURL = indexConfiguration.getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testServiceMap() {
    final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().setIsServiceMap(true).build();
    final URL expTemplateURL = indexConfiguration
            .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(SERVICE_MAP), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testValidCustom() throws MalformedURLException {
    final String fakeTemplateFilePath = "src/resources/dummy.json";
    final String testIndexAlias = "foo";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
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
    final IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder();
    final Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
    assertEquals("Missing required properties:indexAlias", exception.getMessage());
  }

  @Test
  public void testReadIndexConfigRaw() {
    final PluginSetting pluginSetting = generatePluginSetting(
            true, null, null, null, null);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
    final URL expTemplateFile = indexConfiguration
            .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
    assertEquals(RAW, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateURL());
    assertEquals(5, indexConfiguration.getBulkSize());
  }

  @Test
  public void testReadIndexConfigServiceMap() {
    final PluginSetting pluginSetting = generatePluginSetting(
            null, true, null, null, null);
    final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
    final URL expTemplateFile = indexConfiguration
            .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
    assertEquals(SERVICE_MAP, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(SERVICE_MAP), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateURL());
    assertEquals(5, indexConfiguration.getBulkSize());
  }

  @Test
  public void testReadIndexConfigInvalid() {
    final PluginSetting pluginSetting = generatePluginSetting(
            true, true, null, null, null);
    Exception e = assertThrows(IllegalStateException.class, () -> IndexConfiguration.readIndexConfig(pluginSetting));
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
