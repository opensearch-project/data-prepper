package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.JiraServiceTest.createJiraConfigurationFromYaml;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;

@ExtendWith(MockitoExtension.class)
public class JiraOauthConfigTest {

    @Mock
    RestTemplate restTemplateMock;

    JiraSourceConfig jiraSourceConfig = createJiraConfigurationFromYaml("oauth2-auth-jira-pipeline.yaml");

    @Test
    void testRenewToken() throws InterruptedException {
        Instant testStartTime = Instant.now();
        Map<String, Object> firstMockResponseMap = Map.of("access_token", "first_mock_access_token",
                "refresh_token", "first_mock_refresh_token",
                "expires_in", 3600);
        JiraOauthConfig jiraOauthConfig = new JiraOauthConfig(jiraSourceConfig);
        when(restTemplateMock.postForEntity(any(String.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(new ResponseEntity<>(firstMockResponseMap, HttpStatus.OK));
        jiraOauthConfig.restTemplate = restTemplateMock;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> firstCall = executor.submit(jiraOauthConfig::renewCredentials);
        Future<?> secondCall = executor.submit(jiraOauthConfig::renewCredentials);
        while (!firstCall.isDone() || !secondCall.isDone()) {
            // Do nothing. Wait for the calls to complete
            Thread.sleep(10);
        }
        executor.shutdown();
        assertNotNull(jiraOauthConfig.getAccessToken());
        assertNotNull(jiraOauthConfig.getExpireTime());
        assertEquals(jiraOauthConfig.getRefreshToken(), "first_mock_refresh_token");
        assertEquals(jiraOauthConfig.getExpiresInSeconds(), 3600);
        assertEquals(jiraOauthConfig.getAccessToken(), "first_mock_access_token");
        assertTrue(jiraOauthConfig.getExpireTime().isAfter(testStartTime));
        Instant expectedNewExpireTime = Instant.ofEpochMilli(testStartTime.toEpochMilli() + 3601 * 1000);
        assertTrue(jiraOauthConfig.getExpireTime().isBefore(expectedNewExpireTime),
                String.format("Expected that %s time should be before %s", jiraOauthConfig.getExpireTime(), expectedNewExpireTime));
        verify(restTemplateMock, times(1)).postForEntity(any(String.class), any(HttpEntity.class), any(Class.class));

    }

    @Test
    void testFailedToRenewAccessToken() {
        JiraOauthConfig jiraOauthConfig = new JiraOauthConfig(jiraSourceConfig);
        when(restTemplateMock.postForEntity(any(String.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(HttpClientErrorException.class);
        jiraOauthConfig.restTemplate = restTemplateMock;
        assertThrows(RuntimeException.class, jiraOauthConfig::renewCredentials);
    }


    @Test
    void testGetJiraAccountCloudId() throws InterruptedException {
        Map<String, Object> mockGetCallResponse = new HashMap<>();
        mockGetCallResponse.put("id", "test_cloud_id");
        when(restTemplateMock.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(new ResponseEntity<>(List.of(mockGetCallResponse), HttpStatus.OK));
        JiraOauthConfig jiraOauthConfig = new JiraOauthConfig(jiraSourceConfig);
        jiraOauthConfig.restTemplate = restTemplateMock;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> firstCall = executor.submit(jiraOauthConfig::getUrl);
        Future<?> secondCall = executor.submit(jiraOauthConfig::getUrl);
        while (!firstCall.isDone() || !secondCall.isDone()) {
            // Do nothing. Wait for the calls to complete
            Thread.sleep(10);
        }
        executor.shutdown();

        assertEquals("test_cloud_id", jiraOauthConfig.getJiraAccountCloudId());
        assertEquals("https://api.atlassian.com/ex/jira/test_cloud_id/", jiraOauthConfig.getUrl());
        //calling second time shouldn't trigger rest call
        jiraOauthConfig.getUrl();
        verify(restTemplateMock, times(1))
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
    }

    @Test
    void testGetJiraAccountCloudIdUnauthorizedCase() {

        when(restTemplateMock.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED));
        Map<String, Object> mockRenewTokenResponse = Map.of("access_token", "first_mock_access_token",
                "refresh_token", "first_mock_refresh_token",
                "expires_in", 3600);
        when(restTemplateMock.postForEntity(any(String.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(new ResponseEntity<>(mockRenewTokenResponse, HttpStatus.OK));
        JiraOauthConfig jiraOauthConfig = new JiraOauthConfig(jiraSourceConfig);
        jiraOauthConfig.restTemplate = restTemplateMock;


        assertThrows(UnAuthorizedException.class, () -> jiraOauthConfig.initCredentials());
        verify(restTemplateMock, times(6))
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(restTemplateMock, times(1))
                .postForEntity(any(String.class), any(HttpEntity.class), any(Class.class));

    }

    @Test
    void testFailedToGetCloudId() {
        when(restTemplateMock.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED))
                .thenThrow(HttpClientErrorException.class);
        JiraOauthConfig jiraOauthConfig = new JiraOauthConfig(jiraSourceConfig);
        jiraOauthConfig.restTemplate = restTemplateMock;
        assertThrows(RuntimeException.class, jiraOauthConfig::getUrl);
        for (int i = 0; i <= RETRY_ATTEMPT; i++) {
            assertThrows(RuntimeException.class, jiraOauthConfig::getUrl);
        }
    }

}
