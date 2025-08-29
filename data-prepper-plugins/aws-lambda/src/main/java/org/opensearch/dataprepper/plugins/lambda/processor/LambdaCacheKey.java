/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;
import java.util.ArrayList;

public class LambdaCacheKey {
    private List<Object> keys;

    public LambdaCacheKey(final List<Object> keys) {
        this.keys = keys;
    }

    public LambdaCacheKey(final Event event, final List<String> keyNames) {
        this.keys = new ArrayList<>();
        for (final String keyName : keyNames) {
            Object value = event.get(keyName, Object.class);
            if (!((value instanceof String) || (value instanceof Long) || (value instanceof Integer))) {
                throw new RuntimeException("Only String and Number values are supported in cache");
            }
            this.keys.add(value);
        }
    }

    public int length() {
        return keys.stream()
                .mapToInt(key -> {
                    if (key instanceof String) {
                        return ((String) key).length();
                    } else { // instance of Long/Integer
                        return 8;
                    }
                })
                .sum();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LambdaCacheKey)) return false;
        return keys.equals(((LambdaCacheKey)o).keys);
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }

}
