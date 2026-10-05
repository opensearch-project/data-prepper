/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable settings that control how a map of fields is merged into an {@link Event}
 * via {@link Event#mergeFields(String, Map, MergeSettings)}.
 *
 * @since 2.17
 */
public class MergeSettings {
    private final FieldConflictStrategy conflictStrategy;
    private final boolean replaceInvalidCharacters;

    /**
     * @param conflictStrategy         the strategy used to resolve conflicts with an existing destination
     * @param replaceInvalidCharacters flag indicating if invalid characters should be replaced when writing keys
     */
    public MergeSettings(final FieldConflictStrategy conflictStrategy, final boolean replaceInvalidCharacters) {
        this.conflictStrategy = Objects.requireNonNull(conflictStrategy, "conflictStrategy must not be null");
        this.replaceInvalidCharacters = replaceInvalidCharacters;
    }

    /**
     * @return the conflict resolution strategy to apply when a destination field already exists
     */
    public FieldConflictStrategy getConflictStrategy() {
        return conflictStrategy;
    }

    /**
     * @return whether invalid characters should be replaced when writing keys
     */
    public boolean isReplaceInvalidCharacters() {
        return replaceInvalidCharacters;
    }
}
