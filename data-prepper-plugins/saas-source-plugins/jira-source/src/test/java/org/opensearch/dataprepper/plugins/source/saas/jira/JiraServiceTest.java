package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import javax.inject.Named;


/**
 * The type Jira service.
 */

@Named
public class JiraServiceTest {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JiraServiceTest.class);

  @Test
  public void testFetchingJiraEntities(){

  }

  private JiraConfiguration createJiraConfiguration() {
    JiraSourceConfig jiraSourceConfig = new JiraSourceConfig();
    return JiraConfiguration.of(jiraSourceConfig);
  }

}
