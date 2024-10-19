package org.opensearch.dataprepper.plugins.source.saas.jira.rest.auth;

public interface JiraAuthConfig {

    String getUrl();
    void initCredentials();
    void resetCredentials();
}
