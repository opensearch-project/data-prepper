/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines the conflict resolution strategy used when writing fields into an
 * {@link Event} under a destination that already exists.
 *
 * @since 2.17
 */
public enum FieldConflictStrategy {

    /**
     * Skip writing entirely if the destination already exists.
     */
    SKIP("skip", false),

    /**
     * Overwrite the entire destination with the new parsed map.
     */
    OVERWRITE("overwrite", true),

    /**
     * Merge parsed fields into the existing destination map per-field.
     * If a key conflict occurs, preserve the existing value.
     */
    MERGE_PRESERVE_EXISTING_KEYS("merge_preserve_existing_keys", false),

    /**
     * Merge parsed fields into the existing destination map per-field.
     * If a key conflict occurs, overwrite with the new parsed value.
     */
    MERGE_OVERWRITE_EXISTING_KEYS("merge_overwrite_existing_keys", true);

    private static final Map<String, FieldConflictStrategy> NAMES_MAP = Arrays.stream(FieldConflictStrategy.values())
            .collect(Collectors.toMap(
                    value -> value.getOptionName().toLowerCase(),
                    value -> value
            ));

    private final String optionName;
    private final boolean overwrite;

    FieldConflictStrategy(final String optionName, final boolean overwrite) {
        this.optionName = optionName;
        this.overwrite = overwrite;
    }

    @JsonValue
    public String getOptionName() {
        return optionName;
    }

    /**
     * Whether this strategy overwrites conflicting values. Applies both when replacing an entire
     * destination and when merging per-field: {@code true} for {@link #OVERWRITE} and
     * {@link #MERGE_OVERWRITE_EXISTING_KEYS}, {@code false} for {@link #SKIP} and
     * {@link #MERGE_PRESERVE_EXISTING_KEYS}.
     *
     * @return true if conflicting values are overwritten
     */
    public boolean isOverwrite() {
        return overwrite;
    }

    @JsonCreator
    public static FieldConflictStrategy fromOptionName(final String optionName) {
        if (optionName == null) {
            return null;
        }
        final FieldConflictStrategy strategy = NAMES_MAP.get(optionName.toLowerCase());
        if (strategy == null) {
            throw new IllegalArgumentException(
                    "Invalid destination_conflict_strategy: \"" + optionName +
                    "\". Valid values are: " + NAMES_MAP.keySet());
        }
        return strategy;
    }
}
