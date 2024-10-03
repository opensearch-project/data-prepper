package org.opensearch.dataprepper.plugins.source.saas.jira.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ContentType;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.Item;
import org.opensearch.dataprepper.plugins.source.saas.jira.JiraConfiguration;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CONTENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.TITLE;

/**
 * The type Issue item.
 */
@Value
@Builder
@Slf4j
public class IssueItem implements Item {
  IssueBean issue;
  JiraConfiguration configuration;
  InputStream inputStream;
  String projectKey;
  String projectName;
  String url;
  String id;
  List<String> authors;
  String status;
  String assignee;
  long createdAt;
  long updatedAt;
  Map<String, Object> customMetadata;
  Boolean isCustomMetadataPresent;


  @Override
  public String getDocumentId() {
    return issue.getId();
  }

  @Override
  public InputStream getDocumentBody() {
    return inputStream;
  }

  @Override
  public String getDocumentTitle() {
    return (String) issue.getFields().get(TITLE);
  }

  @Override
  public ContentType getContentType() {
    return (ContentType) issue.getFields().get(CONTENT_TYPE);
  }
}
