/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.utils;

public class CqlConstants {
    public static final String GREATER_THAN = ">";
    public static final String CLOSING_ROUND_BRACKET = ")";

    public static final String SPACE_IN = " AND space in (";
    public static final String SPACE_NOT_IN = " AND space not in (";
    public static final String DELIMITER = "\",\"";
    public static final String PREFIX = "\"";
    public static final String SUFFIX = "\"";
    public static final String CONTENT_TYPE_IN = " AND type in (";
    public static final String CONTENT_TYPE_NOT_IN = " AND type not in (";
    public static final String CQL_FIELD = "cql";
    public static final String EXPAND_FIELD = "expand";
    public static final String EXPAND_VALUE = "all,space,history.lastUpdated";
}
