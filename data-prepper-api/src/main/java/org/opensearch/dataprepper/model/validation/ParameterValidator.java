/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.validation;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ParameterValidator {

     public void validate(final List<String> requiredKeys,
                                final List<String> requiredNonEmptyKeys,
                                final List<String> requiredNonNullKeys,
                                final Map<String, Object> data) {
        requiredKeys.forEach(key -> {
            checkState(data.containsKey(key), key + " need to be assigned");
        });

        requiredNonEmptyKeys.forEach(key -> {
            final String value = (String) data.get(key);
            checkNotNull(value, key + " cannot be null");
            checkArgument(!value.isEmpty(), key + " cannot be an empty string");
        });

        requiredNonNullKeys.forEach(key -> {
            final Object value = data.get(key);
            checkNotNull(value, key + " cannot be null");
        });
    }

}
