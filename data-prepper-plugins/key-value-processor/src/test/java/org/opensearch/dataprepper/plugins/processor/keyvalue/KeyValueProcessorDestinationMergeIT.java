/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.FieldConflictStrategy;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Integration test for the key_value processor's {@code destination_conflict_strategy} behavior.
 * Unlike the unit tests, this deserializes a real {@link KeyValueProcessorConfig} from a settings
 * map and drives a real {@link KeyValueProcessor}, exercising the config binding
 * (YAML key -> enum -> strategy resolution -> write) end-to-end.
 */
class KeyValueProcessorDestinationMergeIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TEST_SOURCE = "body";
    private static final String TEST_DESTINATION = "attributes";

    private KeyValueProcessor createProcessor(final Map<String, Object> extraSettings) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("source", TEST_SOURCE);
        settings.put("destination", TEST_DESTINATION);
        settings.put("field_split_characters", " ");
        settings.put("value_split_characters", "=");
        settings.putAll(extraSettings);

        final KeyValueProcessorConfig config = OBJECT_MAPPER.convertValue(settings, KeyValueProcessorConfig.class);
        return new KeyValueProcessor(mock(PluginMetrics.class), config, mock(ExpressionEvaluator.class));
    }

    private KeyValueProcessor createProcessor(final FieldConflictStrategy strategy) {
        return createProcessor(Map.of("destination_conflict_strategy", strategy.getOptionName()));
    }

    private Record<Event> createEventWithExistingAttributes(final String body, final Map<String, Object> existingAttributes) {
        final Map<String, Object> data = new HashMap<>();
        data.put(TEST_SOURCE, body);
        data.put(TEST_DESTINATION, existingAttributes);
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Record<Event> createEventWithBodyOnly(final String body) {
        final Map<String, Object> data = new HashMap<>();
        data.put(TEST_SOURCE, body);
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @SuppressWarnings("unchecked")
    private Event process(final KeyValueProcessor processor, final Record<Event> record) {
        final List<Record<Event>> results =
                (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        return results.get(0).getData();
    }

    @Test
    void test_merge_preserve_existing_keys_merges_new_fields_without_overwriting_conflicts() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("obs_body_length", 42);

        final Event event = process(processor, createEventWithExistingAttributes(
                "logtype=ws:access method=GET uri=/health", existingAttributes));

        // Existing fields preserved
        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.get("attributes/obs_body_length", Object.class), is(42));

        // New fields merged in
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/method", Object.class), is("GET"));
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void test_merge_preserve_existing_keys_does_not_overwrite_conflicting_keys() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("method", "POST");  // conflicts with parsed value

        final Event event = process(processor, createEventWithExistingAttributes(
                "method=GET uri=/health", existingAttributes));

        // Conflicting field NOT overwritten
        assertThat(event.get("attributes/method", Object.class), is("POST"));
        // Non-conflicting field merged
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void test_merge_overwrite_existing_keys_overwrites_conflicting_fields() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS);

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("method", "POST");  // This should be overwritten

        final Event event = process(processor, createEventWithExistingAttributes(
                "logtype=ws:access method=GET uri=/health", existingAttributes));

        // Existing non-conflicting field preserved
        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        // Conflicting field overwritten
        assertThat(event.get("attributes/method", Object.class), is("GET"));
        // New fields merged in
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void test_skip_strategy_skips_when_destination_exists() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.SKIP);

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");

        final Event event = process(processor, createEventWithExistingAttributes(
                "logtype=ws:access method=GET", existingAttributes));

        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.containsKey("attributes/logtype"), is(false));
        assertThat(event.containsKey("attributes/method"), is(false));
    }

    @Test
    void test_overwrite_strategy_replaces_entire_destination() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.OVERWRITE);

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("obs_body_length", 42);

        final Event event = process(processor, createEventWithExistingAttributes(
                "logtype=ws:access method=GET uri=/health", existingAttributes));

        // Entire destination replaced - existing fields lost
        assertThat(event.containsKey("attributes/cluster_name"), is(false));
        assertThat(event.containsKey("attributes/obs_body_length"), is(false));
        // New parsed fields present
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/method", Object.class), is("GET"));
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void test_destination_does_not_exist_creates_it_regardless_of_strategy() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final Event event = process(processor, createEventWithBodyOnly("key1=value1 key2=value2"));

        assertThat(event.get("attributes/key1", Object.class), is("value1"));
        assertThat(event.get("attributes/key2", Object.class), is("value2"));
    }

    @Test
    void test_merge_preserve_on_non_map_destination_skips_write() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final Map<String, Object> data = new HashMap<>();
        data.put(TEST_SOURCE, "key1=value1 key2=value2");
        data.put(TEST_DESTINATION, "a plain string");
        final Event event = process(processor, new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build()));

        assertThat(event.get(TEST_DESTINATION, Object.class), equalTo("a plain string"));
    }

    @Test
    void test_merge_overwrite_on_non_map_destination_replaces_with_parsed_map() {
        final KeyValueProcessor processor = createProcessor(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS);

        final Map<String, Object> data = new HashMap<>();
        data.put(TEST_SOURCE, "key1=value1 key2=value2");
        data.put(TEST_DESTINATION, "a plain string");
        final Event event = process(processor, new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build()));

        assertThat(event.get("attributes/key1", Object.class), is("value1"));
        assertThat(event.get("attributes/key2", Object.class), is("value2"));
    }

    @Test
    void test_legacy_overwrite_false_without_strategy_resolves_to_skip_behavior() {
        // Backward compatibility: overwrite_if_destination_exists: false should behave as SKIP
        final KeyValueProcessor processor = createProcessor(Map.of("overwrite_if_destination_exists", false));

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");

        final Event event = process(processor, createEventWithExistingAttributes(
                "logtype=ws:access method=GET", existingAttributes));

        // Destination exists, legacy overwrite=false -> skip entirely (backward compatible)
        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.containsKey("attributes/logtype"), is(false));
        assertThat(event.containsKey("attributes/method"), is(false));
    }

    @Test
    void test_legacy_overwrite_true_and_strategy_together_is_rejected() {
        // The two options are mutually exclusive and must fail fast at construction.
        final Map<String, Object> settings = Map.of(
                "overwrite_if_destination_exists", true,
                "destination_conflict_strategy", FieldConflictStrategy.OVERWRITE.getOptionName());

        org.junit.jupiter.api.Assertions.assertThrows(InvalidPluginConfigurationException.class,
                () -> createProcessor(settings));
    }
}
