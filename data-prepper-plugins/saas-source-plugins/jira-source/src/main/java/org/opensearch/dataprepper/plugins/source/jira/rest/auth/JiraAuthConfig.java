package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

/**
 * The interface that defines the behaviour for Jira auth configs.
 */
public interface JiraAuthConfig {

    /**
     * Returns the URL for the Jira instance.
     *
     * @return the URL for the Jira instance.
     */
    String getUrl();

    /**
     * Initializes the credentials for the Jira instance.
     */
    void initCredentials();

    /**
     * Renews the credentials for the Jira instance.
     */
    void renewCredentials();
}
