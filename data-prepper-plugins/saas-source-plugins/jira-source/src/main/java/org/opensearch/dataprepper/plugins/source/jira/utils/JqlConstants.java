/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.utils;

public class JqlConstants {
    public static final String GREATER_THAN = ">";
    public static final String CLOSING_ROUND_BRACKET = ")";

    public static final String SLASH = "/";
    public static final String PROJECT_IN = " AND project in (";
    public static final String STATUS_IN = " AND status in (";
    public static final String PROJECT_NOT_IN = " AND project not in (";
    public static final String STATUS_NOT_IN = " AND status not in (";
    public static final String DELIMITER = "\",\"";
    public static final String PREFIX = "\"";
    public static final String SUFFIX = "\"";
    public static final String ISSUE_TYPE_IN = " AND issueType in (";
    public static final String ISSUE_TYPE_NOT_IN = " AND issueType not in (";
    public static final String JQL_FIELD = "jql";
    public static final String EXPAND_FIELD = "expand";
    public static final String EXPAND_VALUE = "all";
}
