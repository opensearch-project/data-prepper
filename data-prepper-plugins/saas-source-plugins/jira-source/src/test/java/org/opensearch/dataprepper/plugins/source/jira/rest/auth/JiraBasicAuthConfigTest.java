package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals(jiraBasicAuthConfig.getUrl(), jiraSourceConfig.getAccountUrl());
    }

    @Test
    void DoNothingForBasicAuthentication() {
        jiraBasicAuthConfig.initCredentials();
        jiraBasicAuthConfig.renewCredentials();
    }
}