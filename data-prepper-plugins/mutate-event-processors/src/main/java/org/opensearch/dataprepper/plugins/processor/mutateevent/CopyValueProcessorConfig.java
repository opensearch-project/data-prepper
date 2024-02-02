/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopyValueProcessorConfig {
    enum Mode {
        NORMAL("normal"),
        LIST("list");

        private final String name;

        private static final Map<String, Mode> ACTIONS_MAP = Arrays.stream(Mode.values())
                .collect(Collectors.toMap(
                        value -> value.name,
                        value -> value
                ));

        Mode(String name) {
            this.name = name.toLowerCase();
        }

        @JsonCreator
        static Mode fromOptionValue(final String option) {
            return ACTIONS_MAP.get(option);
        }
    }

    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        private String fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        private String toKey;

        @JsonProperty("copy_when")
        private String copyWhen;

        @JsonProperty("overwrite_if_to_key_exists")
        private boolean overwriteIfToKeyExists = false;

        public String getFromKey() {
            return fromKey;
        }

        public String getToKey() {
            return toKey;
        }

        public boolean getOverwriteIfToKeyExists() {
            return overwriteIfToKeyExists;
        }

        public String getCopyWhen() { return copyWhen; }

        public Entry(final String fromKey, final String toKey, final boolean overwriteIfToKeyExists, final String copyWhen) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.overwriteIfToKeyExists = overwriteIfToKeyExists;
            this.copyWhen = copyWhen;
        }

        public Entry() {

        }
    }

    @NotEmpty
    @NotNull
    @Valid
    private List<Entry> entries;

    @JsonProperty("source")
    private String source;

    @JsonProperty("target")
    private String target;

    @JsonProperty("mode")
    private Mode mode = Mode.NORMAL;

    public List<Entry> getEntries() {
        return entries;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public Mode getMode() {
        return mode;
    }
}
