/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import java.util.List;

class CopyValueConversion extends AbstractConversion<CopyValueConversion.CopyValueConfig> {
    public static class CopyValueConfig {
        public final String from_key;
        public final String to_key;

        public CopyValueConfig(final String from_key, final String to_key) {
            this.from_key = from_key;
            this.to_key = to_key;
        }
    }

    public static String getLogstashName() {
        return "copy";
    }

    @Override
    protected void addKvToEntries(final String key, final Object value) {
        entries.add(new CopyValueConfig(key, (String) value));
    }

    @Override
    protected void addListToEntries(final List<String> list) { }

    @Override
    protected String getDataPrepperName() {
        return "copy_values";
    }

    @Override
    protected String getMapKey() {
        return "entries";
    }
}
