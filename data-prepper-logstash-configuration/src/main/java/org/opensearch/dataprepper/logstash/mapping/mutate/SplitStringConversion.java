/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import java.util.List;

class SplitStringConversion extends AbstractConversion<SplitStringConversion.SplitStringConfig> {
    public static class SplitStringConfig {
        public final String source;
        public final String delimiter;

        public SplitStringConfig(final String source, final String delimiter) {
            this.source = source;
            this.delimiter = delimiter;
        }
    }

    public static String getLogstashName() {
        return "split";
    }

    @Override
    protected void addKvToEntries(final String key, final Object value) {
        entries.add(new SplitStringConfig(key, (String) value));
    }

    @Override
    protected void addListToEntries(final List<String> list) { }

    @Override
    protected String getDataPrepperName() {
        return "split_string";
    }

    @Override
    protected String getMapKey() {
        return "entries";
    }
}
