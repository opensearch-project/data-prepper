/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import org.opensearch.dataprepper.model.event.JacksonEvent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

public class JacksonStandardSpan extends JacksonSpan {
    protected JacksonStandardSpan(final Builder builder) {
        super(builder);
    }
    
    @Override
    protected void validateParameters() {
        Map<String, Object> data = toMap();
        REQUIRED_NON_EMPTY_KEYS.forEach(key -> {
            final String value = (String) data.get(key);
            checkNotNull(value, key + " cannot be null");
            checkArgument(!value.isEmpty(), key + " cannot be an empty string");
        });

    }

    @Override
    protected void checkAndSetDefaultValues() {
        Map<String, Object> data = toMap();
        data.computeIfAbsent(ATTRIBUTES_KEY, k -> new HashMap<>());
    }

    @Override
    public String toJsonString() {
        return ((JacksonEvent)this).toJsonString();
    }

}
