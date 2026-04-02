/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "add_entries", pluginType = Processor.class)
class AddEntryProcessorIT extends BaseDataPrepperPluginStandardTestSuite {

    @Test
    void disable_root_keys_resolves_value_expression_from_root(
            @PluginConfigurationFile("iterate_on_with_disable_root_keys.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest) {
        final Map<String, Object> data = new HashMap<>();
        data.put("alert_title", "SQL Injection Detected");
        data.put("vulns", Arrays.asList(
                new HashMap<>(Map.of("cve", "CVE-2024-001")),
                new HashMap<>(Map.of("cve", "CVE-2024-002"))));

        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.execute(Collections.singletonList(record));

        List<Map<String, Object>> vulns = result.get(0).getData().get("vulns", List.class);
        assertThat(vulns.size(), equalTo(2));
        assertThat(vulns.get(0).get("title"), equalTo("SQL Injection Detected"));
        assertThat(vulns.get(1).get("title"), equalTo("SQL Injection Detected"));
    }

    @Test
    void evaluate_when_on_element_filters_per_element(
            @PluginConfigurationFile("iterate_on_with_evaluate_when_on_element.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest) {
        final Map<String, Object> data = new HashMap<>();
        data.put("vulns", Arrays.asList(
                new HashMap<>(Map.of("cve", "CVE-2024-001", "severity", "critical")),
                new HashMap<>(Map.of("cve", "CVE-2024-002", "severity", "low")),
                new HashMap<>(Map.of("cve", "CVE-2024-003", "severity", "critical"))));

        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.execute(Collections.singletonList(record));

        List<Map<String, Object>> vulns = result.get(0).getData().get("vulns", List.class);
        assertThat(vulns.size(), equalTo(3));
        assertThat(vulns.get(0).get("flagged"), equalTo(true));
        assertThat(vulns.get(1).containsKey("flagged"), is(false));
        assertThat(vulns.get(2).get("flagged"), equalTo(true));
    }

    @Test
    void both_flags_combined_resolves_root_value_with_per_element_condition(
            @PluginConfigurationFile("iterate_on_with_both_flags.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest) {
        final Map<String, Object> data = new HashMap<>();
        data.put("alert_title", "SQL Injection Detected");
        data.put("vulns", Arrays.asList(
                new HashMap<>(Map.of("cve", "CVE-2024-001", "severity", "critical")),
                new HashMap<>(Map.of("cve", "CVE-2024-002", "severity", "low")),
                new HashMap<>(Map.of("cve", "CVE-2024-003", "severity", "critical"))));

        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.execute(Collections.singletonList(record));

        List<Map<String, Object>> vulns = result.get(0).getData().get("vulns", List.class);
        assertThat(vulns.size(), equalTo(3));
        assertThat(vulns.get(0).get("title"), equalTo("SQL Injection Detected"));
        assertThat(vulns.get(1).containsKey("title"), is(false));
        assertThat(vulns.get(2).get("title"), equalTo("SQL Injection Detected"));
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
