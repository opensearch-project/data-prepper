package org.opensearch.dataprepper.plugins.source.jira.utils;


import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

import java.util.List;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

/**
 * The type Jira configuration.
 */
@Slf4j
public class JiraConfigHelper {

    public static final String ISSUE_STATUS_FILTER = "status";
    public static final String ISSUE_TYPE_FILTER = "issuetype";


    /**
     * Get Issue Status Filter from repository configuration.
     *
     * @return List Issue Status Filter.
     */
    public static List<String> getIssueStatusFilter(JiraSourceConfig repositoryConfiguration) {
        return repositoryConfiguration.getStatus();
    }

    /**
     * Get Issue Types Filter from repository configuration.
     *
     * @return List Issue Type Filter.
     */
    public static List<String> getIssueTypeFilter(JiraSourceConfig repositoryConfiguration) {
        return repositoryConfiguration.getIssueType();
    }

    /**
     * Get Project Filter Types from repository configuration.
     * public static final String ST = "status";
     *
     * @return List Project Filter.
     */
    public static List<String> getProjectKeyFilter(JiraSourceConfig repositoryConfiguration) {
        return repositoryConfiguration.getProject();
    }


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
            if (config.getJiraId() == null || config.getJiraCredential() == null) {
                throw new RuntimeException("Jira ID or Credential are required for Basic AuthType");
            }
        }

        if (OAUTH2.equals(authType)) {
            if (config.getAccessToken() == null || config.getRefreshToken() == null) {
                throw new RuntimeException("Access Token or Refresh Token are required for OAuth2 AuthType");
            }
        }

        AddressValidation.validateInetAddress(AddressValidation
                .getInetAddress(config.getAccountUrl()));
        return true;
    }
}
