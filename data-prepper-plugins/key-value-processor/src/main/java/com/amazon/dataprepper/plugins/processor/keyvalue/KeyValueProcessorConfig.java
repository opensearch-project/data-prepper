/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.amazon.dataprepper.model.configuration.PluginSetting;

public class KeyValueProcessorConfig {
    static final String SOURCE = "source";
    static final String DESTINATION = "destination";
    static final String FIELD_DELIMITER_REGEX = "field_delimiter_regex";
    static final String KEY_VALUE_DELIMITER_REGEX = "key_value_delimiter_regex";
    static final String NON_MATCH_VALUE = "non_match_value";
    static final String ALLOW_DUPLICATE_VALUES = "allow_duplicate_values";
    static final String PREFIX = "prefix";

    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DESTINATION = "parsed_message";
    static final String DEFAULT_FIELD_DELIMITER_REGEX = "&";
    static final String DEFAULT_KEY_VALUE_DELIMITER_REGEX = ":";
    static final String DEFAULT_NON_MATCH_VALUE = null;
    static final boolean DEFAULT_ALLOW_DUPLICATE_VALUES = true;
    static final String DEFAULT_PREFIX = "";


    private final String source;
    private final String destination;
    private final String field_delimiter_regex;
    private final String key_value_delimiter_regex;
    private final String non_match_value;
    private final boolean allow_duplicate_values;
    private final String prefix;

    private KeyValueProcessorConfig(final String source,
                              final String destination,
                              final String field_delimiter_regex,
                              final String key_value_delimiter_regex,
                              final String non_match_value,
                              final boolean allow_duplicate_values,
                              final String prefix) {

        this.source = source;
        this.destination = destination;
        this.field_delimiter_regex = field_delimiter_regex;
        this.key_value_delimiter_regex = key_value_delimiter_regex;
        this.non_match_value = non_match_value;
        this.allow_duplicate_values = allow_duplicate_values;
        this.prefix = prefix;
    }

    public static KeyValueProcessorConfig buildConfig(final PluginSetting pluginSetting) {
        return new KeyValueProcessorConfig(pluginSetting.getStringOrDefault(SOURCE, DEFAULT_SOURCE),
                pluginSetting.getStringOrDefault(DESTINATION, DEFAULT_DESTINATION),
                pluginSetting.getStringOrDefault(FIELD_DELIMITER_REGEX, DEFAULT_FIELD_DELIMITER_REGEX),
                pluginSetting.getStringOrDefault(KEY_VALUE_DELIMITER_REGEX, DEFAULT_KEY_VALUE_DELIMITER_REGEX),
                pluginSetting.getStringOrDefault(NON_MATCH_VALUE, DEFAULT_NON_MATCH_VALUE),
                pluginSetting.getBooleanOrDefault(ALLOW_DUPLICATE_VALUES, DEFAULT_ALLOW_DUPLICATE_VALUES),
                pluginSetting.getStringOrDefault(PREFIX, DEFAULT_PREFIX));
    }

    public String getSource() {
        return source;
    }

    public String getDestination() {
        return destination;
    }

    public String getFieldDelimiterRegex() {
        return field_delimiter_regex;
    }

    public String getKeyValueDelimiterRegex() {
        return key_value_delimiter_regex;
    }

    public String getNonMatchValue() {
        return non_match_value;
    }

    public boolean getAllowDuplicateValues() {
        return allow_duplicate_values;
    }

    public String getPrefix() {
        return prefix;
    }
}
