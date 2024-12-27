package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig.DEFAULT_NUMBER_OF_WORKERS;

public class JiraSourceConfigTest {
    private final String accessToken = "access token test";
    private final String refreshToken = "refresh token test";
    private final String clientId = "client id test";
    private final String clientSecret = "client secret test";
    private final String password = "test Jira Credential";
    private final String username = "test Jira Id";
    private final String accountUrl = "https://example.atlassian.net";
    private List<String> projectList = new ArrayList<>();
    private List<String> issueTypeList = new ArrayList<>();
    private List<String> inclusionPatternList = new ArrayList<>();
    private List<String> exclusionPatternList = new ArrayList<>();
    private List<String> statusList = new ArrayList<>();
    private JiraSourceConfig jiraSourceConfig;

    private JiraSourceConfig createJiraSourceConfig(String authtype, boolean hasToken) throws JsonProcessingException {
        Map<String, Object> configMap = new HashMap<>();
        List<String> hosts = new ArrayList<>();
        hosts.add(accountUrl);

        configMap.put("hosts", hosts);

        Map<String, Object> authenticationMap = new HashMap<>();
        Map<String, String> basicMap = new HashMap<>();
        Map<String, String> oauth2Map = new HashMap<>();
        if (authtype.equals(BASIC)) {
            basicMap.put("username", username);
            basicMap.put("password", password);
            authenticationMap.put("basic", basicMap);
        } else if (authtype.equals(OAUTH2)) {
            if (hasToken) {
                oauth2Map.put("access_token", accessToken);
                oauth2Map.put("refresh_token", refreshToken);
            } else {
                oauth2Map.put("access_token", null);
                oauth2Map.put("refresh_token", null);
            }
            oauth2Map.put("client_id", clientId);
            oauth2Map.put("client_secret", clientSecret);
            authenticationMap.put("oauth2", oauth2Map);
        }

        configMap.put("authentication", authenticationMap);

        projectList.add("project1");
        projectList.add("project2");
        configMap.put("projects", projectList);

        issueTypeList.add("issue type 1");
        issueTypeList.add("issue type 2");
        configMap.put("issue_types", issueTypeList);

        inclusionPatternList.add("pattern 1");
        inclusionPatternList.add("pattern 2");
        configMap.put("inclusion_patterns", inclusionPatternList);

        exclusionPatternList.add("pattern 3");
        exclusionPatternList.add("pattern 4");
        configMap.put("exclusion_patterns", exclusionPatternList);

        statusList.add("status 1");
        statusList.add("status 2");
        configMap.put("statuses", statusList);

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfig = objectMapper.writeValueAsString(configMap);
        JiraSourceConfig config = objectMapper.readValue(jsonConfig, JiraSourceConfig.class);
        return config;
    }

    @Test
    void testGetters() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(BASIC, false);
        assertEquals(jiraSourceConfig.getInclusionPatterns(), inclusionPatternList);
        assertEquals(jiraSourceConfig.getIssueType(), issueTypeList);
        assertEquals(jiraSourceConfig.getExclusionPatterns(), exclusionPatternList);
        assertEquals(jiraSourceConfig.getNumWorkers(), DEFAULT_NUMBER_OF_WORKERS);
        assertEquals(jiraSourceConfig.getProject(), projectList);
        assertEquals(jiraSourceConfig.getStatus(), statusList);
        assertEquals(jiraSourceConfig.getAccountUrl(), accountUrl);
        assertNotNull(jiraSourceConfig.getBackOff());
        assertEquals(jiraSourceConfig.getAuthenticationConfig().getBasicConfig().getPassword(), password);
        assertEquals(jiraSourceConfig.getAuthenticationConfig().getBasicConfig().getUsername(), username);
    }

    @Test
    void testFetchGivenOauthAttributeWrongAuthType() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(BASIC, true);
        assertThrows(RuntimeException.class, () -> jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
    }

    @Test
    void testFetchGivenOauthAtrribute() throws JsonProcessingException {
        jiraSourceConfig = createJiraSourceConfig(OAUTH2, true);
        assertEquals(accessToken, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
        assertEquals(refreshToken, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getRefreshToken());
        assertEquals(clientId, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientId());
        assertEquals(clientSecret, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientSecret());
    }

}
