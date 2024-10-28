package org.opensearch.dataprepper.plugins.source.jira.utils;

/**
 * The type Constants.
 */
public class Constants {

    public static final String TOKEN_LOCATION = "https://auth.atlassian.com/oauth/token";
    public static final int TOKEN_EXPIRED = 401;
    public static final int SUCCESS_RESPONSE = 200;
    public static final int BAD_RESPONSE = 400;
    public static final int RETRY_ATTEMPT = 6;
    public static final String OAuth2_URL = "https://api.atlassian.com/ex/jira/";
    public static final String ACCESSIBLE_RESOURCES = "https://api.atlassian.com/oauth/token/accessible-resources";
    public static final String CONTENT_TYPE = "ContentType";
    public static final String KEY = "key";
    public static final String NAME = "name";
    public static final String PROJECT = "project";
    public static final String OAUTH2 = "OAuth2";
    public static final String _PROJECT = "project-";
    public static final String _ISSUE = "ISSUE-";
    public static final String UPDATED = "updated";
    public static final String PROJECT_KEY = "j_project_key";
    public static final String PROJECT_NAME = "j_project_name";
    public static final String ISSUE_KEY = "j_issue_key";
    public static final String CREATED = "created";
    public static final String BASIC = "Basic";
    public static final String LIVE = "live";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String ERR_MSG = "errorMessages";
    public static final String PLUGIN_NAME = "jira";

    public static final String BAD_REQUEST_EXCEPTION = "Bad request exception occurred "
            + "Invalid project key found in filter configuration for ";

    public static final String PRINT_NULL = "null";

    public static final String NEW_LINE = "\n";

    public static final String NEW_LINE_WITH_SPACE = "\n    ";
    public static final String CLOSING_BRACKET = "}";
    public static final String GREATER_THAN_EQUALS = ">=";
    public static final String CLOSING_ROUND_BRACKET = ")";

    public static final String SLASH = "/";
    public static final String PROJECT_IN = "&project in (";
    public static final String STATUS_IN = "&status in (";
    public static final String DELIMITER = "\",\"";
    public static final String PREFIX = "\"";
    public static final String SUFFIX = "\"";
    public static final String REST_API_SEARCH = "rest/api/3/search";
    public static final String REST_API_FETCH_ISSUE = "rest/api/3/issue";
    public static final String MAX_RESULT = "maxResults";
    public static final String MAX_RESULTS_WITH_SPACE = "    maxResults: ";
    public static final String HEAD_WITH_SPACE = "    startAt: ";
    public static final String SCHEMA = "    schema: ";
    public static final String ISSUE_TYPE_ID = "    issueTypeIds: ";
    public static final String SEARCH_RESULTS = "class SearchResults {\n";
    public static final String _CONFIG = "    _configuration: ";
    public static final String CUSTOM_ID = "    customId: ";
    public static final String TOTAL = "    total: ";
    public static final String ITEMS_WITH_SPACE = "    items: ";
    public static final String WARN_MSG = "    warningMessages: ";
    public static final String TYPE_WITH_SPACE = "    type: ";
    public static final String JSON_TYPE_BEAN = "class JsonTypeBean {\n";
    public static final String INVALID_URL = "URL is not valid ";
    public static final String CUSTOM = "    custom: ";
    public static final String _SYSTEM = "    system: ";
    public static final String EXPAND_WITH_SPACE = "    expand: ";


    public static final String ACCEPT = "Accept";
    public static final String Application_JSON = "application/json";
    public static final String MAX_RESULTS = "maxResults";
    public static final String FIFTY = "50";
    public static final String START_AT = "startAt";
    public static final String JQL_FIELD = "jql";
    public static final String EXPAND_FIELD = "expand";
    public static final String EXPAND_VALUE = "all";
    public static final String AUTHORIZATION_ERROR_CODE = "403";
    public static final int MAX_CHARACTERS_LENGTH = 1000;


    public static final String ISSUE = "ISSUE";


    public static final String SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER =
            String.format("JIRA Issue Status Filter list size"
                            + " should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
    public static final String SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER_OBJECT_VALUE =
            String.format("JIRA Issue Status Filter characters length "
                            + "should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
    public static final String SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER =
            String.format("JIRA Issue Type Filter list size"
                            + " should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
    public static final String SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER_OBJECT_VALUE =
            String.format("JIRA Issue Type Filter characters length "
                            + "should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
    public static final String SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER =
            String.format("JIRA Project Key Filter list size"
                            + " should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
    public static final String SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER_OBJECT_VALUE =
            String.format("JIRA Project Key Filter characters length "
                            + "should not be greater than %s.",
                    MAX_CHARACTERS_LENGTH);
}