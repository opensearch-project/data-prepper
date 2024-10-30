package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper.ISSUE_STATUS_FILTER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper.ISSUE_TYPE_FILTER;

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
    void testIssueTypeFilter() {
        testGetIssue(ISSUE_TYPE_FILTER);
    }

    @Test
    void testIssueStatusFilter() {
        testGetIssue(ISSUE_STATUS_FILTER);
    }

    private void testGetIssue(String filter) {
        List<String> issueTypeFilter = List.of("Bug", "Task");
        when(jiraSourceConfig.getAdditionalProperties()).thenReturn(
                Map.of(filter, issueTypeFilter)
        );
        List<String> result = null;
        if (filter.equals(ISSUE_TYPE_FILTER)) {
            result = JiraConfigHelper.getIssueTypeFilter(jiraSourceConfig);
        } else if (filter.equals(ISSUE_STATUS_FILTER)) {
            result = JiraConfigHelper.getIssueStatusFilter(jiraSourceConfig);
        }
        assertEquals(issueTypeFilter, result);
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
