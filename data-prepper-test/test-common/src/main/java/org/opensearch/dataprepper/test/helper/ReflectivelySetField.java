/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.helper;

import java.lang.reflect.Field;

/**
 * Utility class to reflectively set the field of a JSON-based configuration class. Useful for testing input validation behavior.
 */
public class ReflectivelySetField {
    private ReflectivelySetField() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Reflectively set the field of a configuration object.
     * @param <T> The type
     * @param configurationClass The class of the configuration object.
     * @param configurationObject The configuration object itself (for tests you might need to specify
     *                            "NameOfTestClass.this.configurationObject"
     * @param fieldName Field to change
     * @param value Value that field is set to
     *
     * @throws NoSuchFieldException When field does not exist
     * @throws IllegalAccessException When field cannot be accessed
     */
    public static <T> void setField(final Class<T> configurationClass, final Object configurationObject,
                                       final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
            final Field field = configurationClass.getDeclaredField(fieldName);
            try {
                field.setAccessible(true);
                field.set(configurationObject, value);
            } finally {
                field.setAccessible(false);
            }
    }
}
