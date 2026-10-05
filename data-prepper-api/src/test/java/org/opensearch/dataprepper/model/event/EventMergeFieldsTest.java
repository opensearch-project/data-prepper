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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventMergeFieldsTest {
    private static final String EVENT_TYPE = "event";

    private Event newEvent() {
        return JacksonEvent.builder().withEventType(EVENT_TYPE).build();
    }

    private Event newEvent(final Map<String, Object> data) {
        return JacksonEvent.builder().withEventType(EVENT_TYPE).withData(data).build();
    }

    private Map<String, Object> values() {
        final Map<String, Object> values = new HashMap<>();
        values.put("key1", "value1");
        values.put("key2", "value2");
        return values;
    }

    private MergeSettings settings(final FieldConflictStrategy conflictStrategy) {
        return new MergeSettings(conflictStrategy, false);
    }

    @Test
    void mergeSettings_rejects_null_strategy() {
        assertThrows(NullPointerException.class, () -> new MergeSettings(null, false));
    }

    @Test
    void mergeFields_rejects_null_settings() {
        final Event event = newEvent();
        assertThrows(NullPointerException.class,
                () -> event.mergeFields(null, values(), null));
    }

    // ---- Root writes (null / empty destination) ----

    @ParameterizedTest
    @ValueSource(strings = {"", "NULL"})
    void write_toRoot_addsAllKeys(final String destination) {
        final String dest = "NULL".equals(destination) ? null : destination;
        final Event event = newEvent();

        event.mergeFields(dest, values(), settings(FieldConflictStrategy.OVERWRITE));

        assertThat(event.get("key1", String.class), is("value1"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void write_toRoot_withOverwriteStrategy_overwritesExistingKey() {
        final Event event = newEvent(Map.of("key1", "original"));

        event.mergeFields(null, values(), settings(FieldConflictStrategy.OVERWRITE));

        assertThat(event.get("key1", String.class), is("value1"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void write_toRoot_withPreserveStrategy_keepsExistingKey() {
        final Event event = newEvent(Map.of("key1", "original"));

        event.mergeFields(null, values(), settings(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS));

        assertThat(event.get("key1", String.class), is("original"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void write_toRoot_withSkipStrategy_mergesPreservingExistingKey() {
        // At the root SKIP does not skip: it collapses to a preserve merge (isOverwrite() == false).
        final Event event = newEvent(Map.of("key1", "original"));

        event.mergeFields(null, values(), settings(FieldConflictStrategy.SKIP));

        assertThat(event.get("key1", String.class), is("original"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void write_toRoot_withMergeOverwriteStrategy_overwritesExistingKey() {
        final Event event = newEvent(Map.of("key1", "original"));

        event.mergeFields(null, values(), settings(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS));

        assertThat(event.get("key1", String.class), is("value1"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    // ---- Destination does not exist yet ----

    @Test
    void write_toDestination_whenAbsent_writesWholeMap() {
        final Event event = newEvent();

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.SKIP));

        assertThat(event.get("dest/key1", String.class), is("value1"));
        assertThat(event.get("dest/key2", String.class), is("value2"));
    }

    // ---- Destination exists ----

    @Test
    void write_toDestination_skip_leavesExisting() {
        final Event event = newEvent(Map.of("dest", Map.of("existing", "keep")));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.SKIP));

        assertThat(event.get("dest/existing", String.class), is("keep"));
        assertThat(event.containsKey("dest/key1"), is(false));
    }

    @Test
    void write_toDestination_overwrite_replacesEntireValue() {
        final Event event = newEvent(Map.of("dest", Map.of("existing", "gone")));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.OVERWRITE));

        assertThat(event.get("dest/key1", String.class), is("value1"));
        assertThat(event.get("dest/existing", String.class), nullValue());
    }

    @Test
    void write_toDestination_mergePreserve_keepsConflictingFields() {
        final Map<String, Object> existing = new HashMap<>();
        existing.put("key1", "original");
        existing.put("existing", "keep");
        final Event event = newEvent(Map.of("dest", existing));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS));

        assertThat(event.get("dest/key1", String.class), is("original"));
        assertThat(event.get("dest/key2", String.class), is("value2"));
        assertThat(event.get("dest/existing", String.class), is("keep"));
    }

    @Test
    void write_toDestination_mergeOverwrite_overwritesConflictingFields() {
        final Map<String, Object> existing = new HashMap<>();
        existing.put("key1", "original");
        existing.put("existing", "keep");
        final Event event = newEvent(Map.of("dest", existing));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS));

        assertThat(event.get("dest/key1", String.class), is("value1"));
        assertThat(event.get("dest/key2", String.class), is("value2"));
        assertThat(event.get("dest/existing", String.class), is("keep"));
    }

    @Test
    void write_toDestination_mergeOverwrite_intoNonMap_replacesValue() {
        final Event event = newEvent(Map.of("dest", "a plain string"));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS));

        assertThat(event.get("dest/key1", String.class), is("value1"));
        assertThat(event.get("dest/key2", String.class), is("value2"));
    }

    @Test
    void write_toDestination_mergePreserve_intoNonMap_skipsWrite() {
        final Event event = newEvent(Map.of("dest", "a plain string"));

        event.mergeFields("dest", values(), settings(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS));

        assertThat(event.get("dest", String.class), is("a plain string"));
    }

    // ---- Per-field write failure is caught and does not abort the merge ----

    @Test
    void write_toRoot_whenKeyIsInvalid_swallowsExceptionAndWritesRemainingKeys() {
        final Event event = newEvent();
        final Map<String, Object> valuesWithInvalidKey = new HashMap<>();
        valuesWithInvalidKey.put("valid_key", "value1");
        valuesWithInvalidKey.put("invalid&key", "value2");

        assertDoesNotThrow(() ->
                event.mergeFields(null, valuesWithInvalidKey, settings(FieldConflictStrategy.OVERWRITE)));

        assertThat(event.get("valid_key", String.class), is("value1"));
        assertThat(event.toMap().containsKey("invalid&key"), is(false));
    }

    @Test
    void write_toDestination_withUnrecognizedStrategy_throwsIllegalState() {
        final Event event = newEvent(Map.of("dest", Map.of("existing", "keep")));

        assertThrows(IllegalStateException.class,
                () -> event.mergeFields("dest", values(), settings(FieldConflictStrategy.UNKNOWN)));
    }
}
