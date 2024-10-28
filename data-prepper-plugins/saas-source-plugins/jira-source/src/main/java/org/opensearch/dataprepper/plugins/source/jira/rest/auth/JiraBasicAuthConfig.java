package org.opensearch.dataprepper.plugins.source.jira.rest.auth;


import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

public class JiraBasicAuthConfig implements JiraAuthConfig {

    private final JiraSourceConfig jiraSourceConfig;

    public JiraBasicAuthConfig(JiraSourceConfig jiraSourceConfig) {
        this.jiraSourceConfig = jiraSourceConfig;
    }

    @Override
    public String getUrl() {
        return jiraSourceConfig.getAccountUrl();
    }

    @Override
    public void initCredentials() {
        //do nothing for basic authentication
    }

    @Override
    public void renewCredentials() {
        //do nothing for basic authentication
    }


}
