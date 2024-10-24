package org.opensearch.dataprepper.plugins.source.saas.jira.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConstantsTest {
    private Constants constants;

    @Test
    public void testInitialization() {
        constants = new Constants();
        assertNotNull(Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER);
        assertNotNull(constants);
    }

}
