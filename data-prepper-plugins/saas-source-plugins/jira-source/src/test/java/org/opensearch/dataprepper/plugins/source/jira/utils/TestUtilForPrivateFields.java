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

import java.lang.reflect.Field;

/**
 * Utility class to help test private fields
 */
public class TestUtilForPrivateFields {

    // Helper method to set private fields via reflection
    public static void setPrivateField(Object targetObject, String fieldName, Object value)
            throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }
}
