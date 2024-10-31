package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

public class JiraSourceConfigTest {
    private JiraSourceConfig jiraSourceConfig;

    private String accessToken = "access token test";
    private String refreshToken = "refresh token test";
    private String clientId = "client id test";
    private String clientSecret = "client secret test";


    private JiraSourceConfig createJiraSourceConfig(String authtype, boolean hasToken) throws JsonProcessingException {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("account_url", "https://example.atlassian.net");

        Map<String, String> connectorCredentialMap = new HashMap<>();
        connectorCredentialMap.put("auth_type", authtype);
        if (hasToken) {
            connectorCredentialMap.put("access_token", accessToken);
            connectorCredentialMap.put("refresh_token", refreshToken);
        } else {
            connectorCredentialMap.put("refresh_token", "");
        }
        connectorCredentialMap.put("client_id", clientId);
        connectorCredentialMap.put("client_secret", clientSecret);

        configMap.put("connector_credentials", connectorCredentialMap);

        List<String> projectList = Arrays.asList("project1", "project2");
        configMap.put("project", projectList);

        List<String> issueTypeList = Arrays.asList("issue type 1", "issue type 2");
        configMap.put("issue_type", issueTypeList);

        List<String> inclusionPatternList = Arrays.asList("pattern 1", "pattern 2");
        configMap.put("inclusion_patterns", inclusionPatternList);

        List<String> exclusionPatternList = Arrays.asList("pattern 3", "pattern 4");
        configMap.put("exclusion_patterns", exclusionPatternList);

        List<String> statusList = Arrays.asList("status 1", "status 2");
        configMap.put("status", statusList);

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfig = objectMapper.writeValueAsString(configMap);
        JiraSourceConfig config = objectMapper.readValue(jsonConfig, JiraSourceConfig.class);
        return config;
    }

    @Test
    void testGetters() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(BASIC, false);
        assertNotNull(jiraSourceConfig.getInclusionPatterns());
        assertNotNull(jiraSourceConfig.getIssueType());
        assertNotNull(jiraSourceConfig.getExclusionPatterns());
        assertNotNull(jiraSourceConfig.getNumWorkers());
        assertNotNull(jiraSourceConfig.getIssueType());
        assertNotNull(jiraSourceConfig.getProject());
        assertNotNull(jiraSourceConfig.getStatus());
        assertNotNull(jiraSourceConfig.getConnectorCredentials());
        assertNotNull(jiraSourceConfig.getAccountUrl());
        assertNotNull(jiraSourceConfig.getBackOff());
    }

    @Test
    void testFetchGivenOauthAttributeWrongAuthType() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(BASIC, true);
        assertThrows(RuntimeException.class, () -> jiraSourceConfig.getAccessToken());
    }

    @Test
    void testFetchGivenOauthAtrribute() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(OAUTH2, true);
        assertEquals(accessToken, jiraSourceConfig.getAccessToken());
        assertEquals(refreshToken, jiraSourceConfig.getRefreshToken());
        assertEquals(clientId, jiraSourceConfig.getClientId());
        assertEquals(clientSecret, jiraSourceConfig.getClientSecret());
    }

    @Test
    void testFetchGivenOauthAtrributeMissing() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(OAUTH2, false);
        assertThrows(RuntimeException.class, () -> jiraSourceConfig.getAccessToken());
        assertThrows(RuntimeException.class, () -> jiraSourceConfig.getRefreshToken());

    }

}
