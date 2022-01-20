/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AggregateIdentificationKeysHasher {
    AggregateIdentificationKeysHasher() {

    }

    Map<Object, Object> createIdentificationKeyHashFromEvent(final Event event, final List<String> identificationKeys) {
        final Map<Object, Object> identificationKeysHash = new HashMap<>();
        for (final String identificationKey : identificationKeys) {
            identificationKeysHash.put(identificationKey, event.get(identificationKey, Object.class));
        }
        return identificationKeysHash;
    }
}
