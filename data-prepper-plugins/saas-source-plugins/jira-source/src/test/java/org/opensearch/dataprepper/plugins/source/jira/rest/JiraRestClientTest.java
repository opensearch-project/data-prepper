package org.opensearch.dataprepper.plugins.source.jira.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraServiceTest;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class JiraRestClientTest {

    @Mock
    private StringBuilder jql;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private JiraAuthConfig authConfig;

    private static Stream<Arguments> provideHttpStatusCodesWithExceptionClass() {
        return Stream.of(
                Arguments.of(HttpStatus.FORBIDDEN, UnAuthorizedException.class),
                Arguments.of(HttpStatus.UNAUTHORIZED, RuntimeException.class),
                Arguments.of(HttpStatus.TOO_MANY_REQUESTS, RuntimeException.class),
                Arguments.of(HttpStatus.INSUFFICIENT_STORAGE, RuntimeException.class)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"basic-auth-jira-pipeline.yaml"})
    public void testFetchingJiraIssue(String configFileName) {
        String exampleTicketResponse = "{\"id\":\"123\",\"key\":\"key\",\"self\":\"https://example.com/rest/api/2/issue/123\"}";
        doReturn(new ResponseEntity<>(exampleTicketResponse, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        JiraSourceConfig jiraSourceConfig = JiraServiceTest.createJiraConfigurationFromYaml(configFileName);
        JiraAuthConfig authConfig = new JiraAuthFactory(jiraSourceConfig).getObject();
        JiraRestClient jiraRestClient = new JiraRestClient(restTemplate, authConfig);
        String ticketDetails = jiraRestClient.getIssue("key");
        assertEquals(exampleTicketResponse, ticketDetails);
    }

    @ParameterizedTest
    @MethodSource("provideHttpStatusCodesWithExceptionClass")
    void testInvokeRestApiTokenExpired(HttpStatus statusCode, Class expectedExceptionType) {
        JiraRestClient jiraRestClient = new JiraRestClient(restTemplate, authConfig);
        jiraRestClient.setSleepTimeMultiplier(1);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(statusCode));
        assertThrows(expectedExceptionType, () -> jiraRestClient.getIssue("key"));
    }

    @Test
    void testInvokeRestApiTokenExpiredInterruptException() throws InterruptedException {
        JiraRestClient jiraRestClient = new JiraRestClient(restTemplate, authConfig);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));
        jiraRestClient.setSleepTimeMultiplier(100000);

        Thread testThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> {
                try {
                    jiraRestClient.getIssue("key");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });
        testThread.start();
        Thread.sleep(100);
        testThread.interrupt();
    }

    @Test
    public void testGetAllIssuesOauth2() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = JiraServiceTest.createJiraConfiguration(OAUTH2, issueType, issueStatus, projectKey);
        JiraRestClient jiraRestClient = new JiraRestClient(restTemplate, authConfig);
        SearchResults mockSearchResults = mock(SearchResults.class);
        doReturn("http://mock-service.jira.com").when(authConfig).getUrl();
        doReturn(new ResponseEntity<>(mockSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        SearchResults results = jiraRestClient.getAllIssues(jql, 0, jiraSourceConfig);
        assertNotNull(results);
    }

    @Test
    public void testGetAllIssuesBasic() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = JiraServiceTest.createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraRestClient jiraRestClient = new JiraRestClient(restTemplate, authConfig);
        SearchResults mockSearchResults = mock(SearchResults.class);
        doReturn(new ResponseEntity<>(mockSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        SearchResults results = jiraRestClient.getAllIssues(jql, 0, jiraSourceConfig);
        assertNotNull(results);
    }

}
