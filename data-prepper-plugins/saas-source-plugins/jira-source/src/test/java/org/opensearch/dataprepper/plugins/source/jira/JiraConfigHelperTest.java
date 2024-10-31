package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class JiraConfigHelperTest {

    @Mock
    JiraSourceConfig jiraSourceConfig;

    @Test
    void testInitialization() {
        JiraConfigHelper jiraConfigHelper = new JiraConfigHelper();
        assertNotNull(jiraConfigHelper);
    }

    @Test
    void testGetIssueStatusFilter() {
        assertTrue(JiraConfigHelper.getIssueStatusFilter(jiraSourceConfig).isEmpty());
        List<String> issueStatusFilter = List.of("Done", "In Progress");
        when(jiraSourceConfig.getProject()).thenReturn(issueStatusFilter);
        assertEquals(issueStatusFilter, JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig));
    }

    @Test
    void testGetIssueTypeFilter() {
        assertTrue(JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig).isEmpty());
        List<String> issueTypeFilter = List.of("Bug", "Story");
        when(jiraSourceConfig.getProject()).thenReturn(issueTypeFilter);
        assertEquals(issueTypeFilter, JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig));
    }

    @Test
    void testGetProjectKeyFilter() {
        assertTrue(JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig).isEmpty());
        List<String> projectKeyFilter = List.of("TEST", "TEST2");
        when(jiraSourceConfig.getProject()).thenReturn(projectKeyFilter);
        assertEquals(projectKeyFilter, JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig));
    }

    @Test
    void testValidateConfig() {
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAuthType()).thenReturn("fakeType");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }

    @Test
    void testValidateConfigBasic() {
        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(jiraSourceConfig.getAuthType()).thenReturn(BASIC);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getJiraId()).thenReturn("id");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getJiraCredential()).thenReturn("credential");
        when(jiraSourceConfig.getJiraId()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getJiraId()).thenReturn("id");
        assertDoesNotThrow(() -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }

    @Test
    void testValidateConfigOauth2() {
        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(jiraSourceConfig.getAuthType()).thenReturn(OAUTH2);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAccessToken()).thenReturn("id");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getRefreshToken()).thenReturn("credential");
        when(jiraSourceConfig.getAccessToken()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAccessToken()).thenReturn("id");
        assertDoesNotThrow(() -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }
}
