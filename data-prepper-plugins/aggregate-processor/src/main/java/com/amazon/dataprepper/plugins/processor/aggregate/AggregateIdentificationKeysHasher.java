/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class AggregateIdentificationKeysHasher {
    private final List<String> identificationKeys;
    AggregateIdentificationKeysHasher(final List<String> identificationKeys) {
        this.identificationKeys = identificationKeys;
    }

    IdentificationHash createIdentificationKeyHashFromEvent(final Event event) {
        final Map<Object, Object> identificationKeysHash = new HashMap<>();
        for (final String identificationKey : identificationKeys) {
            identificationKeysHash.put(identificationKey, event.get(identificationKey, Object.class));
        }
        return new IdentificationHash(identificationKeysHash);
    }

    public static class IdentificationHash {
        private final Map<Object, Object> hash;

        IdentificationHash(final Map<Object, Object> hash) {
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdentificationHash that = (IdentificationHash) o;
            return Objects.equals(hash, that.hash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash);
        }
    }
}
