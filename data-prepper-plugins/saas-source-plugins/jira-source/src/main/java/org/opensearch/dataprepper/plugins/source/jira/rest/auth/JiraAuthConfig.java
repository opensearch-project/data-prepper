package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

public interface JiraAuthConfig {

    String getUrl();

    void initCredentials();

    void renewCredentials();
}
