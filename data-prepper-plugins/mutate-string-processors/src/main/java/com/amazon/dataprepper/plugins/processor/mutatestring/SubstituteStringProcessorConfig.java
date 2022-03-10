/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import java.util.List;

public class SubstituteStringProcessorConfig implements StringProcessorConfig<SubstituteStringProcessorConfig.Entry> {
    public static class Entry {
        public String source;
        public String from;
        public String to;
    }

    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public List<Entry> getIterativeConfig() {
        return entries;
    }
}
