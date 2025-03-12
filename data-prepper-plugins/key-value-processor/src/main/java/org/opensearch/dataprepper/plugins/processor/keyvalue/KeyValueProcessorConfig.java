/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.annotations.ValidRegex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder
@JsonClassDescription("You can use the <code>key_value</code> processor to create structured data by parsing key-value pairs from strings.")
public class KeyValueProcessorConfig {
    static final String VALUE_GROUPING_KEY = "value_grouping";
    static final String FIELD_DELIMITER_REGEX_KEY = "field_delimiter_regex";
    static final String FIELD_SPLIT_CHARACTERS_KEY = "field_split_characters";
    static final String VALUE_SPLIT_CHARACTERS_KEY = "value_split_characters";
    static final String KEY_VALUE_DELIMITER_REGEX_KEY = "key_value_delimiter_regex";
    static final String REMOVE_BRACKETS_KEY = "remove_brackets";
    static final String SKIP_DUPLICATE_VALUES_KEY = "skip_duplicate_values";
    static final String WHITESPACE_KEY = "whitespace";
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DESTINATION = "parsed_message";
    public static final String DEFAULT_FIELD_SPLIT_CHARACTERS = "&";
    static final List<String> DEFAULT_INCLUDE_KEYS = new ArrayList<>();
    static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();
    static final Map<String, Object> DEFAULT_DEFAULT_VALUES = Map.of();
    public static final String DEFAULT_VALUE_SPLIT_CHARACTERS = "=";
    static final Object DEFAULT_NON_MATCH_VALUE = null;

    @NotEmpty
    @JsonProperty(defaultValue = DEFAULT_SOURCE)
    @JsonPropertyDescription("The source field to parse for key-value pairs. The default value is <code>message</code>.")
    private String source = DEFAULT_SOURCE;

    @JsonProperty(defaultValue = DEFAULT_DESTINATION)
    @JsonPropertyDescription("The destination field for the structured data. The destination will be a structured map with the key value pairs extracted from the source. " +
            "If <code>destination</code> is set to <code>null</code>, the parsed fields will be written to the root of the event. " +
            "The default value is <code>parsed_message</code>.")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty(value = FIELD_SPLIT_CHARACTERS_KEY, defaultValue = DEFAULT_FIELD_SPLIT_CHARACTERS)
    @JsonPropertyDescription("A string of characters specifying the delimiter that separates key-value pairs. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "This field cannot be defined along with <code>field_delimiter_regex</code>. " +
            "The default value is <code>&amp;</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = FIELD_DELIMITER_REGEX_KEY, allowedValues = {"null"}),
    })
    @ExampleValues({
        @Example(value = "&&", description = "{\"key1=value1&&key2=value2\"} parses into {\"key1\": \"value1\", \"key2\": \"value2\"}.")
    })
    private String fieldSplitCharacters = DEFAULT_FIELD_SPLIT_CHARACTERS;

    @ValidRegex(message = "The value of field_delimiter_regex is not a valid regex string")
    @JsonProperty(FIELD_DELIMITER_REGEX_KEY)
    @JsonPropertyDescription("A regular expression specifying the delimiter that separates key-value pairs. " +
            "For example, to split on multiple <code>&amp;</code> characters use <code>&amp;+</code>. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "This field cannot be defined along with <code>field_split_characters</code>. " +
            "If this option is not defined, the <code>key_value</code> processor will parse the source using <code>field_split_characters</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = VALUE_GROUPING_KEY, allowedValues = {"false"}),
            @AlsoRequired.Required(name = FIELD_SPLIT_CHARACTERS_KEY, allowedValues = {"null"})
    })
    @ExampleValues({
        @Example(value = "&\\{2\\}", description = "{\"key1=value1&&key2=value2\"} parses into {\"key1\": \"value1\", \"key2\": \"value2\"}.")
    })
    private String fieldDelimiterRegex;

    @JsonProperty(value = VALUE_SPLIT_CHARACTERS_KEY, defaultValue = DEFAULT_VALUE_SPLIT_CHARACTERS)
    @JsonPropertyDescription("A string of characters specifying the delimiter that separates keys from their values within a key-value pair. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "This field cannot be defined along with <code>key_value_delimiter_regex</code>. " +
            "The default value is <code>=</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = KEY_VALUE_DELIMITER_REGEX_KEY, allowedValues = {"null"})
    })
    @ExampleValues({
        @Example(value = "==", description = "{\"key1==value1=\"} parses into {\"key1\": \"value1\"}.")
    })
    private String valueSplitCharacters = DEFAULT_VALUE_SPLIT_CHARACTERS;

    @ValidRegex(message = "The value of key_value_delimiter_regex is not a valid regex string")
    @JsonProperty(KEY_VALUE_DELIMITER_REGEX_KEY)
    @JsonPropertyDescription("A regular expression specifying the delimiter that separates keys from their values within a key-value pair. " +
            "For example, to split on multiple <code>=</code> characters use <code>=+</code>. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "This field cannot be defined along with <code>value_split_characters</code>. " +
            "If this option is not defined, the <code>key_value</code> processor will parse the source using <code>value_split_characters</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = VALUE_SPLIT_CHARACTERS_KEY, allowedValues = {"null"})
    })
    @ExampleValues({
        @Example(value = "&\\{2\\}", description = "{\"key1==value1=\"} parses into {\"key1\": \"value1\"}.")
    })
    private String keyValueDelimiterRegex;

    @JsonProperty(value = "default_values", defaultValue = "{}")
    @JsonPropertyDescription("A map specifying the default keys and their values that should be added " +
            "to the event in case these keys do not exist in the source field being parsed. " +
            "If the key was parsed from the source field that value will remain and the default value is not used. " +
            "If the default values includes keys which are not part of <code>include_keys</code> those keys and value will be added to the event.")
    @NotNull
    private Map<String, Object> defaultValues = DEFAULT_DEFAULT_VALUES;

    @JsonProperty("non_match_value")
    @JsonPropertyDescription("Configures a value to use when the processor cannot split a key-value pair. " +
            "The value specified in this configuration is the value used in <code>destination</code> map. " +
            "The default behavior is to drop the key-value pair.")
    @ExampleValues({
        @Example(value = "none", description = "key1value1&key2=value2 parses into {\"key1value1\": \"none\", \"key2\": \"value2\"}.")
    })
    private Object nonMatchValue = DEFAULT_NON_MATCH_VALUE;

    @JsonProperty("include_keys")
    @JsonPropertyDescription("An array specifying the keys that should be included in the destination field. " +
            "By default, all keys will be added.")
    @NotNull
    private List<String> includeKeys = DEFAULT_INCLUDE_KEYS;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("An array specifying the parsed keys that should be excluded from the destination field. " +
            "By default, no keys will be excluded.")
    @NotNull
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonPropertyDescription("A prefix to append before all keys. By default no prefix is added.")
    @ExampleValues({
        @Example(value = "query:", description = "{\"key1=value1\"} parses into {\"query:key1\": \"value1\"}.")
    })
    private String prefix = null;

    @ValidRegex(message = "The value of delete_key_regex is not a valid regex string")
    @JsonProperty("delete_key_regex")
    @JsonPropertyDescription("A regular expression specifying characters to delete from the key. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "Cannot be an empty string. " +
            "By default, no characters are deleted from the key.")
    @ExampleValues({
        @Example(value = "\\s", description = "{\"key1 =value1\"} parses into {\"key1\": \"value1\"}.")
    })
    private String deleteKeyRegex;

    @ValidRegex(message = "The value of delete_value_regex is not a valid regex string")
    @JsonProperty("delete_value_regex")
    @JsonPropertyDescription("A regular expression specifying characters to delete from the value. " +
            "Special regular expression characters such as <code>[</code> and <code>]</code> must be escaped with <code>\\\\</code>. " +
            "Cannot be an empty string. " +
            "By default, no characters are deleted from the value.")
    @ExampleValues({
        @Example(value = "\\s", description = "{\"key1=value1 \"} parses into {\"key1\": \"value1\"}.")
    })
    private String deleteValueRegex;

    @JsonProperty(value = "transform_key", defaultValue = "none")
    @JsonPropertyDescription("Allows transforming the key's name such as making the name all lowercase.")
    private TransformOption transformKey = TransformOption.NONE;

    @JsonProperty(value = WHITESPACE_KEY, defaultValue = "lenient")
    @JsonPropertyDescription("Specifies whether to be lenient or strict with the acceptance of " +
            "unnecessary white space surrounding the configured value-split sequence. " +
            "In this case, strict means that whitespace is trimmed and lenient means it is retained in the key name and in the value. " +
            "Default is <code>lenient</code>.")
    @NotNull
    @ExampleValues({
        @Example(value = "lenient", description = "{\"key1 = value1\"} will parse into {\"key1 \": \" value1\"}."),
        @Example(value = "strict", description = "{\"key1 = value1\"} will parse into {\"key1\": \"value1\"}.")
    })
    private WhitespaceOption whitespace = WhitespaceOption.LENIENT;

    @JsonProperty(value = SKIP_DUPLICATE_VALUES_KEY, defaultValue = "false")
    @JsonPropertyDescription("A Boolean option for removing duplicate key-value pairs. When set to <code>true</code>, " +
            "only one unique key-value pair will be preserved. Default is <code>false</code>.")
    @NotNull
    private boolean skipDuplicateValues = false;

    @JsonProperty(value = REMOVE_BRACKETS_KEY, defaultValue = "false")
    @JsonPropertyDescription("Specifies whether to treat certain grouping characters as wrapping text that should be removed from values." +
            "When set to <code>true</code>, the following grouping characters will be removed: square brackets, angle brackets, and parentheses. " +
            "The default configuration is <code>false</code> which retains those grouping characters.")
    private boolean removeBrackets;

    @JsonProperty(value = VALUE_GROUPING_KEY, defaultValue = "false")
    @JsonPropertyDescription("Specifies whether to group values using predefined grouping delimiters. " +
            "If this flag is enabled, then the content between the delimiters is considered to be one entity and " +
            "they are not parsed as key-value pairs. The following characters are used a group delimiters: " +
            "<code>{...}</code>, <code>[...]</code>, <code>&lt;...&gt;</code>, <code>(...)</code>, <code>\"...\"</code>, <code>'...'</code>, <code>http://... (space)</code>, and <code>https:// (space)</code>. " +
            "Default is <code>false</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = FIELD_DELIMITER_REGEX_KEY, allowedValues = {"null"})
    })
    private boolean valueGrouping = false;

    @JsonProperty(value = "recursive", defaultValue = "false")
    @JsonPropertyDescription("Specifies whether to recursively obtain additional key-value pairs from values. " +
            "The extra key-value pairs will be stored as nested objects within the destination object. Default is <code>false</code>. " +
            "The levels of recursive parsing must be defined by different brackets for each level: " +
            "<code>[]</code>, <code>()</code>, and <code>&lt;&gt;</code>, in this order. Any other configurations specified will only be applied " +
            "to the outermost keys.\n" +
            "When <code>recursive</code> is <code>true</code>:\n" +
            "<code>remove_brackets</code> cannot also be <code>true</code>;\n" +
            "<code>skip_duplicate_values</code> will always be <code>true</code>;\n" +
            "<code>whitespace</code> will always be <code>\"strict\"</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = REMOVE_BRACKETS_KEY, allowedValues = {"false"}),
            @AlsoRequired.Required(name = SKIP_DUPLICATE_VALUES_KEY, allowedValues = {"true"}),
            @AlsoRequired.Required(name = WHITESPACE_KEY, allowedValues = {"strict"})
    })
    private boolean recursive = false;
    
    @JsonProperty(value = "overwrite_if_destination_exists", defaultValue = "true")
    @JsonPropertyDescription("Specifies whether to overwrite existing fields if there are key conflicts " +
            "when writing parsed fields to the event. Default is <code>true</code>.")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty(value = "drop_keys_with_no_value", defaultValue = "false")
    @JsonPropertyDescription("Specifies whether keys should be dropped if they have a null value. Default is <code>false</code>. " +
            "For example, if <code>drop_keys_with_no_value</code> is set to <code>true</code>, " +
            "then <code>{\"key1=value1&amp;key2\"}</code> parses to <code>{\"key1\": \"value1\"}</code>.")
    private boolean dropKeysWithNoValue = false;

    @JsonProperty(value = "strict_grouping", defaultValue = "false")
    @JsonPropertyDescription("When enabled, groups with unmatched end characters yield errors. " +
            "The event is ignored after the errors are logged. " +
            "Specifies whether strict grouping should be enabled when the <code>value_grouping</code> " +
            "or <code>string_literal_character</code> options are used. Default is <code>false</code>.")
    private boolean strictGrouping = false;

    @JsonProperty("string_literal_character")
    @JsonPropertyDescription("When this option is used, any text contained within the specified literal " +
            "character will be ignored and excluded from key-value parsing. " +
            "Can be set to either a single quotation mark (<code>'</code>) or a double quotation mark (<code>\"</code>). " +
            "Default is <code>null</code>.")
    @Size(min = 0, max = 1, message = "string_literal_character may only have one character")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = VALUE_GROUPING_KEY, allowedValues = {"true"})
    })
    @ExampleValues({
        @Example(value = "\"", description = "text1 \"key1=value1\" text2 key2=value2 would parse to {\"key2\": \"value2\"}.")
    })
    private String stringLiteralCharacter = null;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("The tags to add to the event metadata if the <code>key_value</code> processor fails to parse the source string.")
    private List<String> tagsOnFailure;

    @JsonProperty("key_value_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/some_key == \"test\"</code>. " +
            "If specified, the <code>key_value</code> processor will only run on events when the expression evaluates to true. ")
    @ExampleValues({
        @Example(value = "/some-key == \"test\"", description = "When the key is 'test', the processor will be applied to the event.")
    })
    private String keyValueWhen;

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

    public TransformOption getTransformKey() {
        return transformKey;
    }

    public WhitespaceOption getWhitespace() {
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
