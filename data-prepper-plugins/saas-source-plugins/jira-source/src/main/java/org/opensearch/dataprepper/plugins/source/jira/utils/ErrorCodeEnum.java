package org.opensearch.dataprepper.plugins.source.jira.utils;

/**
 * Jira error code enums.
 */
public enum ErrorCodeEnum {
    FIELD_SIZE_OVER_MAX_LIMIT(ErrorConstants.JIRA_5101,
            ErrorConstants.FIELD_SIZE_OVER_MAX_LIMIT_MESSAGE),
    ERROR_JIRA_INCLUSION_PATTERN(ErrorConstants.JIRA_5102,
            "JIRA inclusion pattern list size is too large."),
    ERROR_JIRA_EXCLUSION_PATTERN(ErrorConstants.JIRA_5104,
            "JIRA exclusion pattern list size is too large."),
    INCLUSION_PATTERN_OBJECT_VALUE(ErrorConstants.JIRA_5103,
            "Some of the inclusion object exceeding the characters limit."),
    EXCLUSION_PATTERN_OBJECT_VALUE(ErrorConstants.JIRA_5105,
            "Some of the exclusion object exceeding the characters limit."),
    EMPTY_ACCESS_TOKEN(ErrorConstants.JIRA_5100,
            ErrorConstants.EMPTY_ACCESS_TOKEN),
    EMPTY_REFRESH_TOKEN(ErrorConstants.JIRA_5106, ErrorConstants.EMPTY_REFRESH_TOKEN),
    EMPTY_JIRA_CREDENTIAL(ErrorConstants.JIRA_5107, ErrorConstants.EMPTY_JIRA_CREDENTIAL),
    EMPTY_JIRA_ID(ErrorConstants.JIRA_5108, ErrorConstants.EMPTY_JIRA_ID),
    EMPTY_AUTH_TYPE(ErrorConstants.JIRA_5109, ErrorConstants.EMPTY_AUTH_TYPE),
    EMPTY_ACC_URL(ErrorConstants.JIRA_5110, ErrorConstants.EMPTY_ACC_URL),

    ERROR_JIRA_ISSUE_SUB_ENTITY_FILTER_PATTERN("JIRA-5111",
            "JIRA Issue Sub Entity Filter list size is too large."),
    JIRA_ISSUE_ISSUE_SUB_ENTITY_FILTER_VALUE("JIRA-5112",
            "Some of the JIRA Issue Sub Entity Filter object"
                    + "exceeding the characters limit."),
    ERROR_JIRA_ISSUE_STATUS_FILTER_PATTERN("JIRA-5113",
            "JIRA Issue Status Filter list size is too large."),
    JIRA_ISSUE_STATUS_FILTER_VALUE("JIRA-5114",
            "Some of the JIRA Issue Status Filter object"
                    + "exceeding the characters limit."),
    ERROR_JIRA_ISSUE_TYPE_FILTER("JIRA-5115",
            "JIRA Issue Type Filter list size is too large."),
    JIRA_ISSUE_TYPE_FILTER_VALUE("JIRA-5116",
            "Some of the JIRA Issue Type Filter object"
                    + "exceeding the characters limit."),
    ERROR_JIRA_PROJECT_KEY_FILTER("JIRA-5117",
            "JIRA Project Key Filter list size is too large."),
    JIRA_PROJECT_KEY_FILTER_VALUE("JIRA-5118",
            "Some of the JIRA Project Key Filter object"
                    + "exceeding the characters limit."),
    EMPTY_PROJECT("JIRA-5119",
            ErrorConstants.EMPTY_PROJECT_MESSAGE),
    EMPTY_ISSUE("JIRA-5120",
            ErrorConstants.EMPTY_ISSUE_MESSAGE),
    EMPTY_COMMENT("JIRA-5121",
            ErrorConstants.EMPTY_COMMENT_MESSAGE),
    EMPTY_ATTACHMENT("JIRA-5122",
            ErrorConstants.EMPTY_ATTACHMENT_MESSAGE),
    EMPTY_WORKLOG("JIRA-5123",
            ErrorConstants.EMPTY_WORKLOG_MESSAGE),
    EMPTY_CRAWL_TYPE("JIRA-5124", ErrorConstants.EMPTY_CRAWL_TYPE);

    public final String code;
    public final String errorMessage;

    ErrorCodeEnum(final String validationCode, final String validationMessage) {
        this.code = validationCode;
        this.errorMessage = validationMessage;
    }

    /**
     * Method to get error code.
     *
     * @return code
     */
    public String getErrorCode() {
        return code;
    }

    /**
     * Method to get error message.
     *
     * @return error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
