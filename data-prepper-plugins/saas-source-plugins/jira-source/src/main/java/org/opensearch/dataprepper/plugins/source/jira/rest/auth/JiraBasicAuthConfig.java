/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.rest.auth;


import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

public class JiraBasicAuthConfig implements JiraAuthConfig {

    private final JiraSourceConfig jiraSourceConfig;
    private String accountUrl;

    public JiraBasicAuthConfig(JiraSourceConfig jiraSourceConfig) {
        this.jiraSourceConfig = jiraSourceConfig;
        accountUrl = jiraSourceConfig.getAccountUrl();
        if (!accountUrl.endsWith("/")) {
            accountUrl += "/";
        }
    }

    @Override
    public String getUrl() {
        return accountUrl;
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
