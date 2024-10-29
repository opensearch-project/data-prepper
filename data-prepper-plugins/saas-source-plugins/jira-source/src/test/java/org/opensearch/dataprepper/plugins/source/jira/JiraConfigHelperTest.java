package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.exception.BadRequestException;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.JiraConfigHelper.ISSUE_STATUS_FILTER;
import static org.opensearch.dataprepper.plugins.source.jira.JiraConfigHelper.ISSUE_TYPE_FILTER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.MAX_CHARACTERS_LENGTH;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER_OBJECT_VALUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER_OBJECT_VALUE;

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
        testInvalidPattern(ISSUE_TYPE_FILTER);
        testTooManyKeys(ISSUE_TYPE_FILTER);
    }

    @Test
    void testIssueStatusFilter() {
        testGetIssue(ISSUE_STATUS_FILTER);
        testInvalidPattern(ISSUE_STATUS_FILTER);
        testTooManyKeys(ISSUE_STATUS_FILTER);
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

    void testInvalidPattern(String filter) {
        List<String> issueTypeFilter = List.of("Bug", "Task", "a".repeat(MAX_CHARACTERS_LENGTH + 5));
        when(jiraSourceConfig.getAdditionalProperties()).thenReturn(
                Map.of(filter, issueTypeFilter)
        );
        if (filter.equals(ISSUE_TYPE_FILTER)) {
            BadRequestException exception = assertThrows(BadRequestException.class, () -> JiraConfigHelper.getIssueTypeFilter(jiraSourceConfig));
            assertTrue(exception.getMessage().contains(SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER_OBJECT_VALUE));
        } else if (filter.equals(ISSUE_STATUS_FILTER)) {
            BadRequestException exception = assertThrows(BadRequestException.class, () -> JiraConfigHelper.getIssueStatusFilter(jiraSourceConfig));
            assertTrue(exception.getMessage().contains(SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER_OBJECT_VALUE));
        }
    }

    private void testTooManyKeys(String filter) {
        List<String> issueTypeFilter = new java.util.ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            issueTypeFilter.add("Bug" + i);
        }
        when(jiraSourceConfig.getAdditionalProperties()).thenReturn(
                Map.of(filter, issueTypeFilter)
        );
        if (filter.equals(ISSUE_TYPE_FILTER)) {
            BadRequestException exception = assertThrows(BadRequestException.class, () -> JiraConfigHelper.getIssueTypeFilter(jiraSourceConfig));
            assertTrue(exception.getMessage().contains(SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER));
        } else if (filter.equals(ISSUE_STATUS_FILTER)) {
            BadRequestException exception = assertThrows(BadRequestException.class, () -> JiraConfigHelper.getIssueStatusFilter(jiraSourceConfig));
            assertTrue(exception.getMessage().contains(SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER));
        }
    }


    @Test
    void testGetProjectKeyFilter() {
        assertTrue(JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig).isEmpty());
        List<String> projectKeyFilter = List.of("TEST", "TEST2");
        when(jiraSourceConfig.getProject()).thenReturn(projectKeyFilter);
        assertEquals(projectKeyFilter, JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig));
    }

    @Test
    void testGetProjectKeyFilterTooManyKeys() {
        assertTrue(JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig).isEmpty());
        List<String> projectKeyFilter = new java.util.ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            projectKeyFilter.add("TEST" + i);
        }
        when(jiraSourceConfig.getProject()).thenReturn(projectKeyFilter);
        assertThrows(BadRequestException.class, () -> JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig));
    }

    @Test
    void testGetProjectKeyFilterKeyTooLong() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("a".repeat(MAX_CHARACTERS_LENGTH + 10));
        String result = stringBuilder.toString();
        List<String> projectKeyFilter = new java.util.ArrayList<>();
        projectKeyFilter.add(result);

        when(jiraSourceConfig.getProject()).thenReturn(projectKeyFilter);
        assertThrows(BadRequestException.class, () -> JiraConfigHelper.getProjectKeyFilter(jiraSourceConfig).isEmpty());
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
        when(jiraSourceConfig.getAccountUrl()).thenReturn("XXXXXXXXXXXXXXXX");
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
        when(jiraSourceConfig.getAccountUrl()).thenReturn("XXXXXXXXXXXXXXXX");
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
