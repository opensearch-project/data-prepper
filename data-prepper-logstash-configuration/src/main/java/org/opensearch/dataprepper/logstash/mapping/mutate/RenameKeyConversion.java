/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import java.util.List;

class RenameKeyConversion extends AbstractConversion<RenameKeyConversion.RenameEntryConfig> {
    public static class RenameEntryConfig {
        public final String from_key;
        public final String to_key;

        public RenameEntryConfig(final String from_key, final String to_key) {
            this.from_key = from_key;
            this.to_key = to_key;
        }
    }

    public static String getLogstashName() {
        return "rename";
    }

    @Override
    protected void addKvToEntries(final String key, final Object value) {
        entries.add(new RenameEntryConfig(key, (String) value));
    }

    @Override
    protected void addListToEntries(final List<String> list) { }

    @Override
    protected String getDataPrepperName() {
        return "rename_keys";
    }

    @Override
    protected String getMapKey() {
        return "entries";
    }
}
