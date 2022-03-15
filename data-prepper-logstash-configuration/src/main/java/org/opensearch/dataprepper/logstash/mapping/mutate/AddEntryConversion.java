/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import java.util.List;

class AddEntryConversion extends AbstractConversion<AddEntryConversion.AddEntryConfig> {
    public static class AddEntryConfig {
        public final String key;
        public final Object value;

        public AddEntryConfig(final String key, final Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public static String getLogstashName() {
        return "add_field";
    }

    @Override
    protected void addKvToEntries(final String key, final Object value) {
        entries.add(new AddEntryConfig(key, value));
    }

    @Override
    protected void addListToEntries(final List<String> list) { }

    @Override
    protected String getDataPrepperName() {
        return "add_entries";
    }

    @Override
    protected String getMapKey() {
        return "entries";
    }
}
