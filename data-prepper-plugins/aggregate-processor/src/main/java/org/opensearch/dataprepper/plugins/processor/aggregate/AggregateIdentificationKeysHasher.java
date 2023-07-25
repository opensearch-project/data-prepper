/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AggregateIdentificationKeysHasher {
    private final List<String> identificationKeys;
    public AggregateIdentificationKeysHasher(final List<String> identificationKeys) {
        this.identificationKeys = identificationKeys;
    }

    public IdentificationKeysMap createIdentificationKeysMapFromEvent(final Event event) {
        final Map<Object, Object> identificationKeysMap = new HashMap<>();
        for (final String identificationKey : identificationKeys) {
            identificationKeysMap.put(identificationKey, event.get(identificationKey, Object.class));
        }
        return new IdentificationKeysMap(identificationKeysMap);
    }

    public static class IdentificationKeysMap {
        private final Map<Object, Object> keyMap;

        IdentificationKeysMap(final Map<Object, Object> keyMap) {
            this.keyMap = keyMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdentificationKeysMap that = (IdentificationKeysMap) o;
            return Objects.equals(keyMap, that.keyMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyMap);
        }

        Map<Object, Object> getKeyMap() {
            return keyMap;
        }
    }
}
