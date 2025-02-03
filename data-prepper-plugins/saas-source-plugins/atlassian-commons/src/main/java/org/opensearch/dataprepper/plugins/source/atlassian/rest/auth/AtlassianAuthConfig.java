/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest.auth;

/**
 * The interface that defines the behaviour for Jira auth configs.
 */
public interface AtlassianAuthConfig {

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
