package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class JiraAuthFactoryTest {

    @Mock
    private JiraSourceConfig sourceConfig;

    private JiraAuthFactory jiraAuthFactory;

    @BeforeEach
    void setUp() {
        jiraAuthFactory = new JiraAuthFactory(sourceConfig);
    }

    @Test
    void testGetObjectOauth2() {
        when(sourceConfig.getAuthType()).thenReturn(OAUTH2);
        assertInstanceOf(JiraOauthConfig.class, jiraAuthFactory.getObject());
    }

    @Test
    void testGetObjectBasicAuth() {
        assertInstanceOf(JiraBasicAuthConfig.class, jiraAuthFactory.getObject());
    }

    @Test
    void testGetObjectType() {
        assertEquals(JiraAuthConfig.class, jiraAuthFactory.getObjectType());
    }
}
