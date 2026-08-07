/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.performance.tools;

public class PathTarget {
    private static final String PATH_PROPERTY_NAME = "path";
    private static final String DEFAULT_PATH = "/log/ingest";

    public static String getPath() {
        return System.getProperty(PATH_PROPERTY_NAME, DEFAULT_PATH);
    }
}
