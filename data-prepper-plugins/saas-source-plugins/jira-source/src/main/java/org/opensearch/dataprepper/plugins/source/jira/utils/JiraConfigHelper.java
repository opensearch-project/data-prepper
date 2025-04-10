/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.utils;


import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

/**
 * The type Jira configuration.
 */
@Slf4j
public class JiraConfigHelper {
    /**
     * Get Issue Status Filter from repository configuration.
     *
     * @param repositoryConfiguration repo config
     * @return List Issue Status Filter.
     */
    public static List<String> getIssueStatusIncludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getStatusConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getStatusConfig().getInclude();
    }

    public static List<String> getIssueStatusExcludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getStatusConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getStatusConfig().getExclude();
    }

    /**
     * Get Issue Types Filter from repository configuration.
     *
     * @param repositoryConfiguration repo config
     * @return List Issue Type Filter.
     */
    public static List<String> getIssueTypeIncludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getIssueTypeConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getIssueTypeConfig().getInclude();
    }

    public static List<String> getIssueTypeExcludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getIssueTypeConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getIssueTypeConfig().getExclude();
    }

    /**
     * Get Project Filter Types from repository configuration.
     * public static final String ST = "status";
     *
     * @param repositoryConfiguration repo config
     * @return List Project Filter.
     */
    public static List<String> getProjectNameIncludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null ||
                repositoryConfiguration.getFilterConfig().getProjectConfig() == null ||
                repositoryConfiguration.getFilterConfig().getProjectConfig().getNameConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getProjectConfig().getNameConfig().getInclude();
    }

    public static List<String> getProjectNameExcludeFilter(JiraSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null ||
                repositoryConfiguration.getFilterConfig().getProjectConfig() == null ||
                repositoryConfiguration.getFilterConfig().getProjectConfig().getNameConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getProjectConfig().getNameConfig().getExclude();
    }


    /**
     * Validate Jira Configuration
     *
     * @param config holding user pipeline yaml inputs
     * @return boolean indicating pipeline yaml inputs are valid or not
     */
    public static boolean validateConfig(JiraSourceConfig config) {
        if (config.getAccountUrl() == null) {
            throw new RuntimeException("Account URL is missing.");
        }
        //At least one of the AuthType should be present
        if (config.getAuthType() == null) {
            throw new RuntimeException("Authentication Type is missing.");
        }
        String authType = config.getAuthType();
        if (!OAUTH2.equals(authType) && !BASIC.equals(authType)) {
            throw new RuntimeException("Invalid AuthType is given");
        }

        if (BASIC.equals(authType)) {
            if (config.getAuthenticationConfig().getBasicConfig().getUsername() == null || config.getAuthenticationConfig().getBasicConfig().getPassword() == null) {
                throw new RuntimeException("Jira ID or Credential are required for Basic AuthType");
            }
        }

        if (OAUTH2.equals(authType)) {
            if (config.getAuthenticationConfig().getOauth2Config().getAccessToken() == null || config.getAuthenticationConfig().getOauth2Config().getRefreshToken() == null) {
                throw new RuntimeException("Access Token or Refresh Token are required for OAuth2 AuthType");
            }
        }

        AddressValidation.validateInetAddress(AddressValidation
                .getInetAddress(config.getAccountUrl()));
        return true;
    }
}
