package com.amazon.dataprepper.test.helper;

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
     * @param configurationClass
     * @param configurationObject
     * @param fieldName
     * @param value
     * @param <T>
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
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
