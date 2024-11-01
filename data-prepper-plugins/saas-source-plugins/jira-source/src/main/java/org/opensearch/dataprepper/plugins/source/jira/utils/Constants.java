package org.opensearch.dataprepper.plugins.source.jira.utils;

import java.util.List;

/**
 * The type Constants.
 */
public class Constants {

    public static final String TOKEN_LOCATION = "https://auth.atlassian.com/oauth/token";
    public static final int TOKEN_EXPIRED = 401;
    public static final int RATE_LIMIT = 429;
    public static final int AUTHORIZATION_ERROR_CODE = 403;
    public static final int RETRY_ATTEMPT = 6;
    public static final List<Integer> RETRY_ATTEMPT_SLEEP_TIME = List.of(1, 2, 5, 10, 20, 40);
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
    public static final String EXPIRES_IN = "expires_in";
    public static final String PLUGIN_NAME = "jira";

    public static final String BAD_REQUEST_EXCEPTION = "Bad request exception occurred "
            + "Invalid project key found in filter configuration for ";


    public static final String GREATER_THAN_EQUALS = ">=";
    public static final String CLOSING_ROUND_BRACKET = ")";

    public static final String SLASH = "/";
    public static final String PROJECT_IN = " AND project in (";
    public static final String STATUS_IN = " AND status in (";
    public static final String DELIMITER = "\",\"";
    public static final String PREFIX = "\"";
    public static final String SUFFIX = "\"";
    public static final String REST_API_SEARCH = "rest/api/3/search";
    public static final String REST_API_FETCH_ISSUE = "rest/api/3/issue";
    public static final String MAX_RESULT = "maxResults";
    public static final String ISSUE_TYPE_IN = " AND issueType in (";
    public static final String INVALID_URL = "URL is not valid ";


    public static final String FIFTY = "50";
    public static final String START_AT = "startAt";
    public static final String JQL_FIELD = "jql";
    public static final String EXPAND_FIELD = "expand";
    public static final String EXPAND_VALUE = "all";

}