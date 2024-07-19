/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;

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
    @JsonPropertyDescription("The message field to be parsed. Optional. Default value is `message`.")
    private String source = DEFAULT_SOURCE;

    @JsonPropertyDescription("The destination field for the parsed source. The parsed source overwrites the " +
            "preexisting data for that key. Optional. If `destination` is set to `null`, the parsed fields will be " +
            "written to the root of the event. Default value is `parsed_message`.")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("field_delimiter_regex")
    @JsonPropertyDescription("A regular expression specifying the delimiter that separates key-value pairs. " +
            "Special regular expression characters such as `[` and `]` must be escaped with `\\\\`. " +
            "Cannot be defined at the same time as `field_split_characters`. Optional. " +
            "If this option is not defined, `field_split_characters` is used.")
    private String fieldDelimiterRegex;

    @JsonProperty("field_split_characters")
    @JsonPropertyDescription("A string of characters specifying the delimiter that separates key-value pairs. " +
            "Special regular expression characters such as `[` and `]` must be escaped with `\\\\`. " +
            "Cannot be defined at the same time as `field_delimiter_regex`. Optional. Default value is `&`.")
    private String fieldSplitCharacters = DEFAULT_FIELD_SPLIT_CHARACTERS;

    @JsonProperty("include_keys")
    @JsonPropertyDescription("An array specifying the keys that should be added for parsing. " +
            "By default, all keys will be added.")
    @NotNull
    private List<String> includeKeys = DEFAULT_INCLUDE_KEYS;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("An array specifying the parsed keys that should not be added to the event. " +
            "By default, no keys will be excluded.")
    @NotNull
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("default_values")
    @JsonPropertyDescription("A map specifying the default keys and their values that should be added " +
            "to the event in case these keys do not exist in the source field being parsed. " +
            "If the default key already exists in the message, the value is not changed. " +
            "The `include_keys` filter will be applied to the message before `default_values`.")
    @NotNull
    private Map<String, Object> defaultValues = DEFAULT_DEFAULT_VALUES;

    @JsonProperty("key_value_delimiter_regex")
    @JsonPropertyDescription("A regular expression specifying the delimiter that separates the key and value " +
            "within a key-value pair. Special regular expression characters such as `[` and `]` must be escaped with " +
            "`\\\\`. This option cannot be defined at the same time as `value_split_characters`. Optional. " +
            "If this option is not defined, `value_split_characters` is used.")
    private String keyValueDelimiterRegex;

    @JsonProperty("value_split_characters")
    @JsonPropertyDescription("A string of characters specifying the delimiter that separates the key and value within " +
            "a key-value pair. Special regular expression characters such as `[` and `]` must be escaped with `\\\\`. " +
            "Cannot be defined at the same time as `key_value_delimiter_regex`. Optional. Default value is `=`.")
    private String valueSplitCharacters = DEFAULT_VALUE_SPLIT_CHARACTERS;

    @JsonProperty("non_match_value")
    @JsonPropertyDescription("When a key-value pair cannot be successfully split, the key-value pair is " +
            "placed in the `key` field, and the specified value is placed in the `value` field. " +
            "Optional. Default value is `null`.")
    private Object nonMatchValue = DEFAULT_NON_MATCH_VALUE;

    @JsonPropertyDescription("A prefix to append before all keys. Optional. Default value is an empty string.")
    @NotNull
    private String prefix = DEFAULT_PREFIX;

    @JsonProperty("delete_key_regex")
    @JsonPropertyDescription("A regular expression specifying the characters to delete from the key. " +
            "Special regular expression characters such as `[` and `]` must be escaped with `\\\\`. Cannot be an " +
            "empty string. Optional. No default value.")
    @NotNull
    private String deleteKeyRegex = DEFAULT_DELETE_KEY_REGEX;

    @JsonProperty("delete_value_regex")
    @JsonPropertyDescription("A regular expression specifying the characters to delete from the value. " +
            "Special regular expression characters such as `[` and `]` must be escaped with `\\\\`. " +
            "Cannot be an empty string. Optional. No default value.")
    @NotNull
    private String deleteValueRegex = DEFAULT_DELETE_VALUE_REGEX;

    @JsonProperty("transform_key")
    @JsonPropertyDescription("When to lowercase, uppercase, or capitalize keys.")
    @NotNull
    private String transformKey = DEFAULT_TRANSFORM_KEY;

    @JsonProperty("whitespace")
    @JsonPropertyDescription("Specifies whether to be lenient or strict with the acceptance of " +
            "unnecessary white space surrounding the configured value-split sequence. Default is `lenient`.")
    @NotNull
    private String whitespace = DEFAULT_WHITESPACE;

    @JsonProperty("skip_duplicate_values")
    @JsonPropertyDescription("A Boolean option for removing duplicate key-value pairs. When set to `true`, " +
            "only one unique key-value pair will be preserved. Default is `false`.")
    @NotNull
    private boolean skipDuplicateValues = DEFAULT_SKIP_DUPLICATE_VALUES;

    @JsonProperty("remove_brackets")
    @JsonPropertyDescription("Specifies whether to treat square brackets, angle brackets, and parentheses " +
            "as value “wrappers” that should be removed from the value. Default is `false`.")
    @NotNull
    private boolean removeBrackets = DEFAULT_REMOVE_BRACKETS;

    @JsonProperty("value_grouping")
    @JsonPropertyDescription("Specifies whether to group values using predefined value grouping delimiters: " +
            "`{...}`, `[...]`, `<...>`, `(...)`, `\"...\"`, `'...'`, `http://... (space)`, and `https:// (space)`. " +
            "If this flag is enabled, then the content between the delimiters is considered to be one entity and " +
            "is not parsed for key-value pairs. Default is `false`. If `value_grouping` is `true`, then " +
            "`{\"key1=[a=b,c=d]&key2=value2\"}` parses to `{\"key1\": \"[a=b,c=d]\", \"key2\": \"value2\"}`.")
    private boolean valueGrouping = DEFAULT_VALUE_GROUPING;

    @JsonProperty("recursive")
    @JsonPropertyDescription("Specifies whether to recursively obtain additional key-value pairs from values. " +
            "The extra key-value pairs will be stored as sub-keys of the root key. Default is `false`. " +
            "The levels of recursive parsing must be defined by different brackets for each level: " +
            "`[]`, `()`, and `<>`, in this order. Any other configurations specified will only be applied " +
            "to the outmost keys.\n" +
            "When `recursive` is `true`:\n" +
            "`remove_brackets` cannot also be `true`;\n" +
            "`skip_duplicate_values` will always be `true`;\n" +
            "`whitespace` will always be `\"strict\"`.")
    @NotNull
    private boolean recursive = DEFAULT_RECURSIVE;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("When a `kv` operation causes a runtime exception within the processor, " +
            "the operation is safely stopped without crashing the processor, and the event is tagged " +
            "with the provided tags.")
    private List<String> tagsOnFailure;

    @JsonProperty("overwrite_if_destination_exists")
    @JsonPropertyDescription("Specifies whether to overwrite existing fields if there are key conflicts " +
            "when writing parsed fields to the event. Default is `true`.")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty("drop_keys_with_no_value")
    @JsonPropertyDescription("Specifies whether keys should be dropped if they have a null value. Default is `false`. " +
            "If `drop_keys_with_no_value` is set to `true`, " +
            "then `{\"key1=value1&key2\"}` parses to `{\"key1\": \"value1\"}`.")
    private boolean dropKeysWithNoValue = false;

    @JsonProperty("key_value_when")
    @JsonPropertyDescription("Allows you to specify a [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
            "such as `/some-key == \"test\"`, that will be evaluated to determine whether " +
            "the processor should be applied to the event.")
    private String keyValueWhen;

    @JsonProperty("strict_grouping")
    @JsonPropertyDescription("When enabled, groups with unmatched end characters yield errors. " +
            "The event is ignored after the errors are logged. " +
            "Specifies whether strict grouping should be enabled when the `value_grouping` " +
            "or `string_literal_character` options are used. Default is `false`.")
    private boolean strictGrouping = false;

    @JsonProperty("string_literal_character")
    @JsonPropertyDescription("When this option is used, any text contained within the specified quotation " +
            "mark character will be ignored and excluded from key-value parsing. " +
            "Can be set to either a single quotation mark (`'`) or a double quotation mark (`\"`). " +
            "Default is `null`.")
    @Size(min = 0, max = 1, message = "string_literal_character may only have character")
    private String stringLiteralCharacter = null;

    @AssertTrue(message = "Invalid Configuration. value_grouping option and field_delimiter_regex are mutually exclusive")
    boolean isValidValueGroupingAndFieldDelimiterRegex() {
        return (!valueGrouping || fieldDelimiterRegex == null);
    }

    @AssertTrue(message = "Invalid Configuration. String literal character config is valid only when value_grouping is enabled, " +
            "and only double quote (\") and single quote are (') are valid string literal characters.")
    boolean isValidStringLiteralConfig() {
        if (stringLiteralCharacter == null)
            return true;
        if ((!stringLiteralCharacter.equals("\"") &&
                (!stringLiteralCharacter.equals("'"))))
            return false;
        return valueGrouping;
    }

    public String getSource() {
        return source;
    }

    public Character getStringLiteralCharacter() {
        return stringLiteralCharacter == null ? null : stringLiteralCharacter.charAt(0);
    }

    public boolean isStrictGroupingEnabled() {
        return strictGrouping;
    }

    public String getDestination() {
        return destination;
    }

    public boolean getValueGrouping() {
        return valueGrouping;
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
