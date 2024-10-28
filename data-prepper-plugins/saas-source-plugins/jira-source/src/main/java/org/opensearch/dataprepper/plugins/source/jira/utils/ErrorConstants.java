package org.opensearch.dataprepper.plugins.source.jira.utils;

/**
 * This is the ErrorConstants interface class.
 */
public interface ErrorConstants {
    String JIRA_5100 = "JIRA-5100";
    String EMPTY_ACCESS_TOKEN = "There was a problem while retrieving access token."
            + " Access token should not be null or empty.";
    String JIRA_5101 = "Field Size is more than max limit";
    String FIELD_SIZE_OVER_MAX_LIMIT_MESSAGE = "There was an error parsing the field value. "
            + "The size has exceeded the maximum allowable limit. The maximum size permitted is ";
    String JIRA_5102 = "JIRA-5102";
    String JIRA_5103 = "JIRA-5103";
    String JIRA_5104 = "JIRA_5104";
    String JIRA_5105 = "JIRA_5105";
    String JIRA_5106 = "JIRA_5106";
    String EMPTY_REFRESH_TOKEN = "There was a problem while retrieving refresh token."
            + " Refresh token should not be null or empty.";
    String JIRA_5107 = "JIRA_5107";
    String EMPTY_JIRA_CREDENTIAL = "There was a problem while retrieving Jira Credential."
            + " Jira Credential should not be null or empty.";
    String JIRA_5108 = "JIRA_5108";
    String EMPTY_JIRA_ID = "There was a problem while retrieving Jira Id."
            + " Jira Id should not be null or empty.";
    String JIRA_5109 = "JIRA_5109";
    String EMPTY_AUTH_TYPE = "There was a problem while retrieving Auth Type."
            + " Auth Type should not be null or empty.";
    String JIRA_5110 = "JIRA_5110";
    String EMPTY_ACC_URL = "There was a problem while retrieving Jira Account Url."
            + "Jira Account Url should not be null or empty.";
    String EMPTY_PROJECT_MESSAGE = "Project specific field mappings "
            + "are not configured for connector";
    String EMPTY_ISSUE_MESSAGE = "Issue specific field mappings "
            + "are not configured for connector";
    String EMPTY_COMMENT_MESSAGE = "Comment specific field mappings "
            + "are not configured for connector";
    String EMPTY_ATTACHMENT_MESSAGE = "Attachment specific field mappings "
            + "are not configured for connector";
    String EMPTY_WORKLOG_MESSAGE = "Worklog specific field mappings "
            + "are not configured for connector";
    String INVALID_PROJECT_FIELD = "[{}] - Invalid fields in project field mapping: {}";
    String INVALID_ISSUE_FIELD = "[{}] - Invalid fields in Issue field mapping: {}";
    String INVALID_COMMENT_FIELD = "[{}] - Invalid fields in Comment field mapping: {}";
    String INVALID_ATTACHMENT_FIELD = "[{}] - Invalid fields in Attachment field mapping: {}";
    String INVALID_WORKLOG_FIELD = "[{}] - Invalid fields in Worklog field mapping: {}";
    String EMPTY_CRAWL_TYPE = "There was a problem while retrieving crawl type."
            + " Crawl Type should not be null or empty.";
}