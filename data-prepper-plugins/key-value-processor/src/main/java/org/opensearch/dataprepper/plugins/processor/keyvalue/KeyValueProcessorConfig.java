/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KeyValueProcessorConfig {
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DESTINATION = "parsed_message";
    public static final String DEFAULT_FIELD_SPLIT_CHARACTERS = "&";
    static final List<String> DEFAULT_INCLUDE_KEYS = new ArrayList<>();
    static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();
    static final Map<String, Object> DEFAULT_DEFAULT_VALUES = Map.of();
    public static final String DEFAULT_VALUE_SPLIT_CHARACTERS = "=";
    static final Object DEFAULT_NON_MATCH_VALUE = null;
    static final String DEFAULT_PREFIX = "";
    static final String DEFAULT_DELETE_KEY_REGEX = "";
    static final String DEFAULT_DELETE_VALUE_REGEX = "";
    static final String DEFAULT_TRANSFORM_KEY = "";
    static final String DEFAULT_WHITESPACE = "lenient";
    static final boolean DEFAULT_SKIP_DUPLICATE_VALUES = false;
    static final boolean DEFAULT_REMOVE_BRACKETS = false;
    static final boolean DEFAULT_VALUE_GROUPING = false;
    static final boolean DEFAULT_RECURSIVE = false;

    @NotEmpty
    private String source = DEFAULT_SOURCE;

    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("field_delimiter_regex")
    private String fieldDelimiterRegex;

    @JsonProperty("field_split_characters")
    private String fieldSplitCharacters = DEFAULT_FIELD_SPLIT_CHARACTERS;

    @JsonProperty("include_keys")
    @NotNull
    private List<String> includeKeys = DEFAULT_INCLUDE_KEYS;

    @JsonProperty("exclude_keys")
    @NotNull
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("default_values")
    @NotNull
    private Map<String, Object> defaultValues = DEFAULT_DEFAULT_VALUES;

    @JsonProperty("key_value_delimiter_regex")
    private String keyValueDelimiterRegex;

    @JsonProperty("value_split_characters")
    private String valueSplitCharacters = DEFAULT_VALUE_SPLIT_CHARACTERS;

    @JsonProperty("non_match_value")
    private Object nonMatchValue = DEFAULT_NON_MATCH_VALUE;

    @NotNull
    private String prefix = DEFAULT_PREFIX;

    @JsonProperty("delete_key_regex")
    @NotNull
    private String deleteKeyRegex = DEFAULT_DELETE_KEY_REGEX;

    @JsonProperty("delete_value_regex")
    @NotNull
    private String deleteValueRegex = DEFAULT_DELETE_VALUE_REGEX;

    @JsonProperty("transform_key")
    @NotNull
    private String transformKey = DEFAULT_TRANSFORM_KEY;

    @JsonProperty("whitespace")
    @NotNull
    private String whitespace = DEFAULT_WHITESPACE;

    @JsonProperty("skip_duplicate_values")
    @NotNull
    private boolean skipDuplicateValues = DEFAULT_SKIP_DUPLICATE_VALUES;

    @JsonProperty("remove_brackets")
    @NotNull
    private boolean removeBrackets = DEFAULT_REMOVE_BRACKETS;

    @JsonProperty("value_grouping")
    private boolean valueGrouping = DEFAULT_VALUE_GROUPING;

    @JsonProperty("recursive")
    @NotNull
    private boolean recursive = DEFAULT_RECURSIVE;

    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure;

    @JsonProperty("overwrite_if_destination_exists")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty("drop_keys_with_no_value")
    private boolean dropKeysWithNoValue = false;

    @JsonProperty("enable_double_quote_grouping")
    private boolean enableDoubleQuoteGrouping = false;

    @JsonProperty("key_value_when")
    private String keyValueWhen;

    @AssertTrue(message = "Invalid Configuration. value_grouping option and field_delimiter_regex are mutually exclusive")
    boolean isValidValueGroupingAndFieldDelimiterRegex() {
        return (!valueGrouping || fieldDelimiterRegex == null);
    }

    public String getSource() {
        return source;
    }

    public String getDestination() {
        return destination;
    }

    public boolean getValueGrouping() {
        return valueGrouping;
    }

    public boolean getEnableDoubleQuoteGrouping() {
        return enableDoubleQuoteGrouping;
    }

    public String getFieldDelimiterRegex() {
        return fieldDelimiterRegex;
    }

    public String getFieldSplitCharacters() {
        return fieldSplitCharacters;
    }

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public Map<String, Object> getDefaultValues() {
        return defaultValues;
    }

    public boolean getDropKeysWithNoValue() {
        return dropKeysWithNoValue;
    }

    public String getKeyValueDelimiterRegex() {
        return keyValueDelimiterRegex;
    }

    public String getValueSplitCharacters() {
        return valueSplitCharacters;
    }

    public Object getNonMatchValue() {
        return nonMatchValue;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDeleteKeyRegex() {
        return deleteKeyRegex;
    }

    public String getDeleteValueRegex() {
        return deleteValueRegex;
    }

    public String getTransformKey() {
        return transformKey;
    }

    public String getWhitespace() {
        return whitespace;
    }

    public boolean getSkipDuplicateValues() {
        return skipDuplicateValues;
    }

    public boolean getRemoveBrackets() {
        return removeBrackets;
    }

    public boolean getRecursive() {
        return recursive;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    public boolean getOverwriteIfDestinationExists() {
        return overwriteIfDestinationExists;
    }

    public String getKeyValueWhen() { return keyValueWhen; }
}
