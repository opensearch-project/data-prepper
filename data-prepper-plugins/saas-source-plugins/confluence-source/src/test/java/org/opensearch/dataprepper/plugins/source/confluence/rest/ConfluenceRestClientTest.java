/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceServiceTest;
import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceSourceConfig;
import org.opensearch.dataprepper.plugins.source.confluence.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.confluence.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceAuthFactory;
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
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class ConfluenceRestClientTest {

    @Mock
    private StringBuilder jql;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ConfluenceAuthConfig authConfig;

    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("jiraRestClient", "aws");

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
        ConfluenceSourceConfig confluenceSourceConfig = ConfluenceServiceTest.createJiraConfigurationFromYaml(configFileName);
        ConfluenceAuthConfig authConfig = new ConfluenceAuthFactory(confluenceSourceConfig).getObject();
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        String ticketDetails = confluenceRestClient.getContent("key");
        assertEquals(exampleTicketResponse, ticketDetails);
    }

    @ParameterizedTest
    @MethodSource("provideHttpStatusCodesWithExceptionClass")
    void testInvokeRestApiTokenExpired(HttpStatus statusCode, Class expectedExceptionType) {
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        confluenceRestClient.setSleepTimeMultiplier(1);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(statusCode));
        assertThrows(expectedExceptionType, () -> confluenceRestClient.getContent("key"));
    }

    @Test
    void testInvokeRestApiTokenExpiredInterruptException() throws InterruptedException {
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));
        confluenceRestClient.setSleepTimeMultiplier(100000);

        Thread testThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> {
                try {
                    confluenceRestClient.getContent("key");
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
    public void testGetAllContentOauth2() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        ConfluenceSourceConfig confluenceSourceConfig = ConfluenceServiceTest.createJiraConfiguration(OAUTH2, issueType, issueStatus, projectKey);
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);
        doReturn("http://mock-service.jira.com/").when(authConfig).getUrl();
        doReturn(new ResponseEntity<>(mockConfluenceSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        ConfluenceSearchResults results = confluenceRestClient.getAllContent(jql, 0);
        assertNotNull(results);
    }

    @Test
    public void testGetAllContentBasic() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        ConfluenceSourceConfig confluenceSourceConfig = ConfluenceServiceTest.createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);
        when(authConfig.getUrl()).thenReturn("https://example.com/");
        doReturn(new ResponseEntity<>(mockConfluenceSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        ConfluenceSearchResults results = confluenceRestClient.getAllContent(jql, 0);
        assertNotNull(results);
    }

    @Test
    public void testRestApiAddressValidation() throws JsonProcessingException {
        when(authConfig.getUrl()).thenReturn("https://224.0.0.1/");
        ConfluenceRestClient confluenceRestClient = new ConfluenceRestClient(restTemplate, authConfig, pluginMetrics);
        assertThrows(BadRequestException.class, () -> confluenceRestClient.getContent("TEST-1"));
    }

}
