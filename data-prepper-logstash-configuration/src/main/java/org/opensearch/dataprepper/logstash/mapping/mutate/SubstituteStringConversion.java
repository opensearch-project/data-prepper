/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import java.util.List;

class SubstituteStringConversion extends AbstractConversion<SubstituteStringConversion.SubstituteStringConfig> {
    public static class SubstituteStringConfig {
        public final String source;
        public final String from;
        public final String to;

        public SubstituteStringConfig(final String source, final String from, final String to) {
            this.source = source;
            this.from = from;
            this.to = to;
        }
    }

    public static String getLogstashName() {
        return "gsub";
    }

    @Override
    protected void addKvToEntries(final String key, final Object value) {}

    @Override
    protected void addListToEntries(final List<String> list) {
        for(int i = 0; i < list.size(); i += 3) {
            final String gsubSource = list.get(i);
            final String gsubPatternToReplace = list.get(i + 1);
            final String gsubStringToReplaceWith = list.get(i + 2);
            final SubstituteStringConfig newConfig = new SubstituteStringConfig(gsubSource, gsubPatternToReplace, gsubStringToReplaceWith);
            entries.add(newConfig);
        }
    }

    @Override
    protected String getDataPrepperName() {
        return "substitute_string";
    }

    @Override
    protected String getMapKey() {
        return "entries";
    }
}
