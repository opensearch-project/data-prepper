package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IndexConfiguration {
  /**
   * TODO: add index management policy parameters
   */
  public static final String TRACE_ANALYTICS_RAW_FLAG = "trace_analytics_raw";
  public static final String TRACE_ANALYTICS_SERVICE_MAP_FLAG = "trace_analytics_service_map";
  public static final String INDEX_ALIAS = "index_alias";
  public static final String TEMPLATE_FILE = "template_file";
  public static final String BULK_SIZE = "bulk_size";
  public static final String DEFAULT_INDEX_TYPE = IndexConstants.RAW;
  public static final long DEFAULT_BULK_SIZE = 5L;

  private final String indexType;
  private final String indexAlias;
  private final URL templateURL;
  private final long bulkSize;

  public String getIndexType() {
    return indexType;
  }

  public String getIndexAlias() {
    return indexAlias;
  }

  public URL getTemplateURL() {
    return templateURL;
  }

  public long getBulkSize() {
    return bulkSize;
  }

  public static class Builder {
    private boolean isRaw = true;
    private boolean isServiceMap = false;
    private String indexAlias;
    private String templateFile;
    private long bulkSize = DEFAULT_BULK_SIZE;

    public Builder setIsRaw(final Boolean isRaw) {
      checkNotNull(isRaw, "trace_analytics_raw cannot be null.");
      this.isRaw = isRaw;
      return this;
    }

    public Builder setIsServiceMap(final Boolean isServiceMap) {
      checkNotNull(isServiceMap, "trace_analytics_service_map cannot be null.");
      this.isServiceMap = isServiceMap;
      return this;
    }

    public Builder withIndexAlias(final String indexAlias) {
      checkArgument(indexAlias != null, "indexAlias cannot be null.");
      checkArgument(!indexAlias.isEmpty(), "indexAlias cannot be empty");
      this.indexAlias = indexAlias;
      return this;
    }

    public Builder withTemplateFile(final String templateFile) {
      checkArgument(templateFile != null, "templateFile cannot be null.");
      this.templateFile = templateFile;
      return this;
    }

    public Builder withBulkSize(final long bulkSize) {
      this.bulkSize = bulkSize;
      return this;
    }

    public IndexConfiguration build() {
      return new IndexConfiguration(this);
    }
  }

  private IndexConfiguration(final Builder builder) {
    final String indexType;
    if (builder.isRaw && builder.isServiceMap) {
      throw new IllegalStateException("trace_analytics_raw and trace_analytics_service_map cannot be both true.");
    } else if (builder.isRaw) {
      indexType = IndexConstants.RAW;
    } else if (builder.isServiceMap) {
      indexType = IndexConstants.SERVICE_MAP;
    } else {
      indexType = IndexConstants.CUSTOM;
    }
    this.indexType = indexType;

    URL templateURL = null;
    if (builder.templateFile == null) {
      if (indexType.equals(IndexConstants.RAW)) {
        templateURL = getClass().getClassLoader()
            .getResource(IndexConstants.RAW_DEFAULT_TEMPLATE_FILE);
      } else if (indexType.equals(IndexConstants.SERVICE_MAP)) {
        templateURL = getClass().getClassLoader()
            .getResource(IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
      }
    } else {
      try {
        templateURL = new File(builder.templateFile).toURI().toURL();
      } catch (MalformedURLException e) {
        throw new IllegalStateException("Invalid template file path");
      }
    }
    this.templateURL = templateURL;

    String indexAlias = builder.indexAlias;
    if (indexAlias == null) {
      if (IndexConstants.TYPE_TO_DEFAULT_ALIAS.containsKey(indexType)) {
        indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(indexType);
      } else {
        throw new IllegalStateException("Missing required properties:indexAlias");
      }
    }
    this.indexAlias = indexAlias;
    this.bulkSize = builder.bulkSize;
  }

  public static IndexConfiguration readIndexConfig(final PluginSetting pluginSetting) {
    IndexConfiguration.Builder builder = new IndexConfiguration.Builder();
    builder.setIsRaw(pluginSetting.getBooleanOrDefault(TRACE_ANALYTICS_RAW_FLAG, true));
    builder.setIsServiceMap(pluginSetting.getBooleanOrDefault(TRACE_ANALYTICS_SERVICE_MAP_FLAG, false));
    final String indexAlias = pluginSetting.getStringOrDefault(INDEX_ALIAS, null);
    if (indexAlias != null) {
      builder = builder.withIndexAlias(indexAlias);
    }
    final String templateFile = pluginSetting.getStringOrDefault(TEMPLATE_FILE, null);
    if (templateFile != null) {
      builder = builder.withTemplateFile(templateFile);
    }
    final Long batchSize = pluginSetting.getLongOrDefault(BULK_SIZE, DEFAULT_BULK_SIZE);
    builder = builder.withBulkSize(batchSize);
    return builder.build();
  }
}
