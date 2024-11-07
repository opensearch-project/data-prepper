package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JiraBasicAuthConfigTest {

    @Mock
    private JiraSourceConfig jiraSourceConfig;

    private JiraBasicAuthConfig jiraBasicAuthConfig;

    @BeforeEach
    void setUp() {
        jiraBasicAuthConfig = new JiraBasicAuthConfig(jiraSourceConfig);
    }

    @Test
    void testGetUrl() {
        String url = "https://example.com";
        when(jiraSourceConfig.getAccountUrl()).thenReturn(url);
        assertEquals(jiraBasicAuthConfig.getUrl(), url + '/');

        String url2 = "https://example.com/";
        when(jiraSourceConfig.getAccountUrl()).thenReturn(url2);
        assertEquals(jiraBasicAuthConfig.getUrl(), url2);

    }

    @Test
    void DoNothingForBasicAuthentication() {
        jiraBasicAuthConfig.initCredentials();
        jiraBasicAuthConfig.renewCredentials();
    }
}