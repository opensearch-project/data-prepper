package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.common.unit.ByteSizeUnit;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexConfiguration {
  /**
   * TODO: add index management policy parameters
   */
  public static final String INDEX_TYPE = "index_type";
  public static final String INDEX_ALIAS = "index_alias";
  public static final String TEMPLATE_FILE = "template_file";
  public static final String BATCH_SIZE = "batch_size";

  private final String indexType;
  private final String indexAlias;
  private final URL templateURL;
  private final long batchSize;

  public String getIndexType() {
    return indexType;
  }

  public String getIndexAlias() {
    return indexAlias;
  }

  public URL getTemplateURL() {
    return templateURL;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public static class Builder {
    private String indexType = IndexConstants.RAW;
    private String indexAlias;
    private String templateFile;
    private long batchSize = ByteSizeUnit.MB.toBytes(5);

    public Builder withIndexType(final String indexType) {
      checkArgument(indexType != null, "indexType cannot be null.");
      checkArgument( IndexConstants.TYPES.contains(indexType), "Invalid indexType.");
      this.indexType = indexType;
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

    public Builder withBatchSize(final long batchSize) {
      checkArgument(batchSize >= -1, "batchSize cannot be less than -1 bytes.");
      this.batchSize = batchSize;
      return this;
    }

    public IndexConfiguration build() {
      return new IndexConfiguration(this);
    }
  }

  private IndexConfiguration(final Builder builder) {
    this.indexType = builder.indexType;

    URL templateURL = null;
    if (builder.templateFile == null) {
      if (builder.indexType.equals(IndexConstants.RAW)) {
        templateURL = getClass().getClassLoader()
            .getResource(IndexConstants.RAW_DEFAULT_TEMPLATE_FILE);
      } else if (builder.indexType.equals(IndexConstants.SERVICE_MAP)) {
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
      if (IndexConstants.TYPE_TO_DEFAULT_ALIAS.containsKey(builder.indexType)) {
        indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(builder.indexType);
      } else {
        throw new IllegalStateException("Missing required properties:indexAlias");
      }
    }
    this.indexAlias = indexAlias;
    this.batchSize = builder.batchSize;
  }
}
