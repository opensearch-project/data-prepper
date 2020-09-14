package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.CUSTOM;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.RAW;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.RAW_DEFAULT_TEMPLATE_FILE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE;
import static com.amazon.situp.plugins.sink.elasticsearch.IndexConstants.TYPE_TO_DEFAULT_ALIAS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class IndexConfigurationTests {
  @Test
  public void testDefault() {
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().build();
    URL expTemplateFile = indexConfiguration
        .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
    assertEquals(RAW, indexConfiguration.getIndexType());
    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(expTemplateFile, indexConfiguration.getTemplateURL());
    assertEquals(ByteSizeUnit.MB.toBytes(5), indexConfiguration.getBulkSize());
  }

  @Test
  public void testRawAPMSpan() throws MalformedURLException {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(RAW), indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());

    String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
        .withIndexAlias(testIndexAlias).build();
    URL expTemplateURL = indexConfiguration.getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testServiceMap() throws MalformedURLException {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
        .withIndexType(SERVICE_MAP)
        .withTemplateFile(fakeTemplateFilePath)
        .build();

    assertEquals(TYPE_TO_DEFAULT_ALIAS.get(SERVICE_MAP), indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());

    String testIndexAlias = "foo";
    indexConfiguration = new IndexConfiguration.Builder()
        .withIndexType(SERVICE_MAP)
        .withIndexAlias(testIndexAlias)
        .build();
    URL expTemplateURL = indexConfiguration
            .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);

    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(expTemplateURL, indexConfiguration.getTemplateURL());
  }

  @Test
  public void testValidCustom() throws MalformedURLException {
    String fakeTemplateFilePath = "src/resources/dummy.json";
    String testIndexAlias = "foo";
    IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
            .withIndexType(CUSTOM)
            .withIndexAlias(testIndexAlias)
            .withTemplateFile(fakeTemplateFilePath)
            .withBulkSize(10)
            .build();

    assertEquals(CUSTOM, indexConfiguration.getIndexType());
    assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    assertEquals(new File(fakeTemplateFilePath).toURI().toURL(), indexConfiguration.getTemplateURL());
    assertEquals(10, indexConfiguration.getBulkSize());

    indexConfiguration = new IndexConfiguration.Builder()
            .withIndexType(CUSTOM)
            .withIndexAlias(testIndexAlias)
            .withBulkSize(-1)
            .build();
    assertEquals(-1, indexConfiguration.getBulkSize());
  }

  @Test
  public void testInvalidCustom() {
    // Missing index alias
    IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder()
            .withIndexType(CUSTOM);
    Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
    assertEquals("Missing required properties:indexAlias", exception.getMessage());
  }
}
