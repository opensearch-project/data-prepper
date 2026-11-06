/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecMetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HecEventBuilderTest {

    private static final EventFactory EVENT_FACTORY = TestEventFactory.getTestEventFactory();
    private static final String DEFAULT_SOURCETYPE = "httpevent";
    private static final String CHANNEL = "channel-abc";

    private SplunkHecSourceConfig config;

    @BeforeEach
    void setUp() {
        config = mock(SplunkHecSourceConfig.class);
        when(config.isFlattenEvent()).thenReturn(true);
        when(config.getDefaultSourcetype()).thenReturn(DEFAULT_SOURCETYPE);
        lenient().when(config.isWarnFutureTimestamps()).thenReturn(false);
    }

    private HecEventBuilder createBuilder() {
        return new HecEventBuilder(EVENT_FACTORY, config);
    }

    private static HecTokenConfig.HecTokenDefaults defaults(final String index,
                                                            final String sourcetype,
                                                            final String source,
                                                            final String host,
                                                            final Map<String, String> fields) {
        final HecTokenConfig.HecTokenDefaults tokenDefaults = mock(HecTokenConfig.HecTokenDefaults.class);
        lenient().when(tokenDefaults.getIndex()).thenReturn(index);
        lenient().when(tokenDefaults.getSourcetype()).thenReturn(sourcetype);
        lenient().when(tokenDefaults.getSource()).thenReturn(source);
        lenient().when(tokenDefaults.getHost()).thenReturn(host);
        lenient().when(tokenDefaults.getFields()).thenReturn(fields == null ? Map.of() : fields);
        return tokenDefaults;
    }

    @Test
    void constructor_throws_when_eventFactory_is_null() {
        assertThrows(NullPointerException.class, () -> new HecEventBuilder(null, config));
    }

    @Test
    void constructor_throws_when_config_is_null() {
        assertThrows(NullPointerException.class, () -> new HecEventBuilder(EVENT_FACTORY, null));
    }

    @Test
    void buildFromHecEvents_with_string_event_puts_it_on_message() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "hello world")), null, null);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("message", "hello world"));
        assertThat(event.getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
    }

    @Test
    void buildFromHecEvents_with_object_event_flattens_when_flatten_is_enabled() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", Map.of("field1", "a", "field2", 2))), null, null);

        final Map<String, Object> data = records.get(0).getData().toMap();
        assertThat(data, hasEntry("field1", "a"));
        assertThat(data, hasEntry("field2", 2));
        assertThat(data.get("event"), is(nullValue()));
    }

    @Test
    void buildFromHecEvents_with_object_event_nests_when_flatten_is_disabled() {
        when(config.isFlattenEvent()).thenReturn(false);

        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", Map.of("field1", "a"))), null, null);

        final Map<String, Object> data = records.get(0).getData().toMap();
        assertThat(data, hasEntry("event", Map.of("field1", "a")));
        assertThat(data.get("field1"), is(nullValue()));
    }

    @Test
    void buildFromHecEvents_with_non_string_scalar_event_keeps_it_under_event() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", 42)), null, null);

        assertThat(records.get(0).getData().toMap(), hasEntry("event", 42));
    }

    @Test
    void buildFromHecEvents_throws_with_the_index_of_the_envelope_missing_the_event_field() {
        final HecEventBuilder builder = createBuilder();
        final List<Map<String, Object>> parsedEvents = List.of(
                Map.of("event", "first"),
                Map.of("host", "web01"));

        final HecEventValidationException exception = assertThrows(HecEventValidationException.class,
                () -> builder.buildFromHecEvents(parsedEvents, null, null));

        assertThat(exception.getEventNumber(), equalTo(1));
    }

    @Test
    void buildFromHecEvents_uses_the_request_metadata_over_the_token_defaults() {
        final Map<String, Object> hecEvent = Map.of(
                "event", "m",
                "host", "request-host",
                "source", "request-source",
                "sourcetype", "request-sourcetype",
                "index", "request-index");

        final List<Record<Event>> records = createBuilder().buildFromHecEvents(
                List.of(hecEvent),
                defaults("default-index", "default-sourcetype", "default-source", "default-host", null),
                CHANNEL);

        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("host", "request-host"));
        assertThat(event.toMap(), hasEntry("source", "request-source"));
        assertThat(event.toMap(), hasEntry("sourcetype", "request-sourcetype"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.INDEX), equalTo("request-index"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.HOST), equalTo("request-host"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.SOURCE), equalTo("request-source"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.SOURCETYPE), equalTo("request-sourcetype"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.CHANNEL), equalTo(CHANNEL));
    }

    @Test
    void buildFromHecEvents_falls_back_to_the_token_defaults_when_the_request_omits_metadata() {
        final List<Record<Event>> records = createBuilder().buildFromHecEvents(
                List.of(Map.of("event", "m")),
                defaults("default-index", "default-sourcetype", "default-source", "default-host", null),
                null);

        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("host", "default-host"));
        assertThat(event.toMap(), hasEntry("source", "default-source"));
        assertThat(event.toMap(), hasEntry("sourcetype", "default-sourcetype"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.INDEX), equalTo("default-index"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.CHANNEL), is(nullValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void buildFromHecEvents_treats_blank_request_metadata_as_absent(final String blank) {
        final Map<String, Object> hecEvent = Map.of("event", "m", "host", blank);

        final List<Record<Event>> records = createBuilder().buildFromHecEvents(
                List.of(hecEvent), defaults(null, null, null, "default-host", null), null);

        assertThat(records.get(0).getData().toMap(), hasEntry("host", "default-host"));
    }

    @Test
    void buildFromHecEvents_uses_the_configured_default_sourcetype_when_nothing_else_supplies_one() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m")), null, null);

        assertThat(records.get(0).getData().toMap(), hasEntry("sourcetype", DEFAULT_SOURCETYPE));
    }

    @Test
    void buildFromHecEvents_merges_the_request_fields_object() {
        final Map<String, Object> hecEvent = Map.of("event", "m", "fields", Map.of("dc", "us-west"));

        final List<Record<Event>> records = createBuilder().buildFromHecEvents(List.of(hecEvent), null, null);

        assertThat(records.get(0).getData().toMap(), hasEntry("dc", "us-west"));
    }

    @Test
    void buildFromHecEvents_ignores_a_fields_value_that_is_not_an_object() {
        final Map<String, Object> hecEvent = Map.of("event", "m", "fields", "not-an-object");

        final List<Record<Event>> records = createBuilder().buildFromHecEvents(List.of(hecEvent), null, null);

        assertThat(records.get(0).getData().toMap().get("fields"), is(nullValue()));
    }

    @Test
    void buildFromHecEvents_applies_token_default_fields_without_overwriting_request_fields() {
        final Map<String, Object> hecEvent = Map.of("event", "m", "fields", Map.of("shared", "from-request"));

        final List<Record<Event>> records = createBuilder().buildFromHecEvents(
                List.of(hecEvent),
                defaults(null, null, null, null, Map.of("shared", "from-default", "only-default", "v")),
                null);

        final Map<String, Object> data = records.get(0).getData().toMap();
        assertThat(data, hasEntry("shared", "from-request"));
        assertThat(data, hasEntry("only-default", "v"));
    }

    @Test
    void buildFromHecEvents_builds_one_record_per_envelope_in_order() {
        final List<Record<Event>> records = createBuilder().buildFromHecEvents(
                List.of(Map.of("event", "first"), Map.of("event", "second")), null, null);

        assertThat(records, hasSize(2));
        assertThat(List.of(records.get(0).getData().get("message", String.class),
                        records.get(1).getData().get("message", String.class)),
                contains("first", "second"));
    }

    @Test
    void buildFromHecEvents_with_no_envelopes_returns_no_records() {
        assertThat(createBuilder().buildFromHecEvents(List.of(), null, null), hasSize(0));
    }

    @Test
    void buildFromHecEvents_converts_a_numeric_time_to_an_iso_timestamp() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", 1_700_000_000L)), null, null);

        assertThat(records.get(0).getData().toMap(),
                hasEntry("@timestamp", Instant.ofEpochSecond(1_700_000_000L).toString()));
    }

    @Test
    void buildFromHecEvents_converts_a_fractional_time_to_an_iso_timestamp() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", 1_700_000_000.5d)), null, null);

        assertThat(records.get(0).getData().toMap(),
                hasEntry("@timestamp", Instant.ofEpochSecond(1_700_000_000L, 500_000_000L).toString()));
    }

    @Test
    void buildFromHecEvents_parses_a_time_supplied_as_a_string() {
        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", "1700000000")), null, null);

        assertThat(records.get(0).getData().toMap(),
                hasEntry("@timestamp", Instant.ofEpochSecond(1_700_000_000L).toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-number", "Infinity", "NaN", "1e400"})
    void buildFromHecEvents_falls_back_to_now_for_an_unusable_time(final String time) {
        final Instant before = Instant.now();

        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", time)), null, null);

        final Instant actual = Instant.parse(records.get(0).getData().get("@timestamp", String.class));
        assertThat(actual.isBefore(before), is(false));
    }

    @Test
    void buildFromHecEvents_falls_back_to_now_when_the_time_overflows_an_instant() {
        final Instant before = Instant.now();

        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", Double.MAX_VALUE)), null, null);

        final Instant actual = Instant.parse(records.get(0).getData().get("@timestamp", String.class));
        assertThat(actual.isBefore(before), is(false));
    }

    @Test
    void buildFromHecEvents_uses_now_when_the_envelope_has_no_time() {
        final Instant before = Instant.now();

        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m")), null, null);

        final Instant actual = Instant.parse(records.get(0).getData().get("@timestamp", String.class));
        assertThat(actual.isBefore(before), is(false));
    }

    @Test
    void buildFromHecEvents_still_builds_the_event_when_warning_on_future_timestamps() {
        when(config.isWarnFutureTimestamps()).thenReturn(true);
        final long future = Instant.now().plusSeconds(7200).getEpochSecond();

        final List<Record<Event>> records = createBuilder()
                .buildFromHecEvents(List.of(Map.of("event", "m", "time", future)), null, null);

        assertThat(records.get(0).getData().toMap(),
                hasEntry("@timestamp", Instant.ofEpochSecond(future).toString()));
    }

    @Test
    void buildFromRawLines_builds_one_record_per_non_empty_line() {
        final List<Record<Event>> records = createBuilder()
                .buildFromRawLines(new String[] {"line one", "", "line two"}, null, null, null, null, null, null);

        assertThat(records, hasSize(2));
        assertThat(records.get(0).getData().toMap(), hasEntry("message", "line one"));
        assertThat(records.get(1).getData().toMap(), hasEntry("message", "line two"));
    }

    @Test
    void buildFromRawLines_with_only_empty_lines_returns_no_records() {
        assertThat(createBuilder().buildFromRawLines(new String[] {"", ""}, null, null, null, null, null, null),
                hasSize(0));
    }

    @Test
    void buildFromRawLines_applies_the_query_parameter_metadata() {
        final List<Record<Event>> records = createBuilder().buildFromRawLines(
                new String[] {"line"}, "q-index", "q-sourcetype", "q-source", "q-host", null, CHANNEL);

        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("host", "q-host"));
        assertThat(event.toMap(), hasEntry("source", "q-source"));
        assertThat(event.toMap(), hasEntry("sourcetype", "q-sourcetype"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.INDEX), equalTo("q-index"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.HOST), equalTo("q-host"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.SOURCE), equalTo("q-source"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.SOURCETYPE), equalTo("q-sourcetype"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.CHANNEL), equalTo(CHANNEL));
    }

    @Test
    void buildFromRawLines_falls_back_to_the_token_defaults_and_default_fields() {
        final List<Record<Event>> records = createBuilder().buildFromRawLines(
                new String[] {"line"}, null, null, null, null,
                defaults("d-index", "d-sourcetype", "d-source", "d-host", Map.of("dc", "us-west")), null);

        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("host", "d-host"));
        assertThat(event.toMap(), hasEntry("source", "d-source"));
        assertThat(event.toMap(), hasEntry("sourcetype", "d-sourcetype"));
        assertThat(event.toMap(), hasEntry("dc", "us-west"));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.INDEX), equalTo("d-index"));
    }

    @Test
    void buildFromRawLines_uses_the_configured_default_sourcetype_and_sets_a_timestamp() {
        final Instant before = Instant.now();

        final List<Record<Event>> records = createBuilder()
                .buildFromRawLines(new String[] {"line"}, null, null, null, null, null, null);

        final Event event = records.get(0).getData();
        assertThat(event.toMap(), hasEntry("sourcetype", DEFAULT_SOURCETYPE));
        assertThat(Instant.parse(event.get("@timestamp", String.class)).isBefore(before), is(false));
        assertThat(event.getMetadata().getAttribute(HecMetadataKeyAttributes.INDEX), is(nullValue()));
        assertThat(event.toMap().get("message"), is(not(nullValue())));
    }
}
