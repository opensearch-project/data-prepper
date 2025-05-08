/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.utils;


import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.AddressValidation;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

/**
 * The type Confluence configuration.
 */
@Slf4j
public class ConfluenceConfigHelper {

    /**
     * Get Content Types Filter from configuration.
     *
     * @param repositoryConfiguration repo config
     * @return List Content Type Filter.
     */
    public static List<String> getContentTypeIncludeFilter(ConfluenceSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getPageTypeConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getPageTypeConfig().getInclude();
    }

    public static List<String> getContentTypeExcludeFilter(ConfluenceSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null || repositoryConfiguration.getFilterConfig().getPageTypeConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getPageTypeConfig().getExclude();
    }

    /**
     * Get Space Filter Types from configuration.
     *
     * @param repositoryConfiguration repo config
     * @return List Space Filter.
     */
    public static List<String> getSpacesNameIncludeFilter(ConfluenceSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null ||
                repositoryConfiguration.getFilterConfig().getSpaceConfig() == null ||
                repositoryConfiguration.getFilterConfig().getSpaceConfig().getNameConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getSpaceConfig().getNameConfig().getInclude();
    }

    public static List<String> getSpacesNameExcludeFilter(ConfluenceSourceConfig repositoryConfiguration) {
        if (repositoryConfiguration.getFilterConfig() == null ||
                repositoryConfiguration.getFilterConfig().getSpaceConfig() == null ||
                repositoryConfiguration.getFilterConfig().getSpaceConfig().getNameConfig() == null) {
            return new ArrayList<>();
        }
        return repositoryConfiguration.getFilterConfig().getSpaceConfig().getNameConfig().getExclude();
    }


    /**
     * Validate Confluence Configuration
     *
     * @param config holding user pipeline yaml inputs
     * @return boolean indicating pipeline yaml inputs are valid or not
     */
    public static boolean validateConfig(ConfluenceSourceConfig config) {
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
                throw new RuntimeException("Confluence ID or Credential are required for Basic AuthType");
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
