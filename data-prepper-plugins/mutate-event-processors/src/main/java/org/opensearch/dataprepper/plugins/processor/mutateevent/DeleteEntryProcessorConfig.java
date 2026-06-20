/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.opensearch.dataprepper.model.pattern.Pattern;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;
import java.util.stream.Collectors;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "entries", value = "null"), @SchemaProperty(field = "with_keys_regex", value = "null")},
                thenExpect = {@SchemaProperty(field = "with_keys")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "entries", value = "null"), @SchemaProperty(field = "with_keys", value = "null")},
                thenExpect = {@SchemaProperty(field = "with_keys_regex")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "with_keys", value = "null"), @SchemaProperty(field = "with_keys_regex", value = "null")},
                thenExpect = {@SchemaProperty(field = "entries")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>delete_entries</code> processor deletes fields from events. " +
        "You can specify the keys of the fields you want to delete using the <code>with_keys</code> " +
        "or <code>with_keys_regex</code> configuration options. You can only use one per entry. " +
        "Those keys and their values are deleted from the events.")
public class DeleteEntryProcessorConfig {

    @JsonPropertyOrder
    public static class Entry {
        @JsonProperty("with_keys")
        @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
        @JsonPropertyDescription("A list of keys to be deleted.")
        private List<@NotNull @NotEmpty EventKey> withKeys;

        @JsonProperty("with_keys_regex")
        @JsonPropertyDescription("A list of regex patterns to match keys to be deleted.")
        private List<String> withKeysRegex;

        @JsonProperty("exclude_from_delete")
        @JsonPropertyDescription("A list of keys to exclude from deletion when using with_keys_regex.")
        private Set<EventKey> excludeFromDelete;

        @JsonProperty("delete_when")
        @JsonPropertyDescription("Specifies under what condition the deletion should be performed. " +
                "By default, keys are always deleted. Example: <code>/mykey == \"---\"</code>")
        @ExampleValues({
                @Example(value = "/some_key == null", description = "Only runs the deletion if the key some_key is null or does not exist.")
        })
        private String deleteWhen;

        @JsonPropertyDescription("Specifies the condition for when to delete the key from each element of a list when using iterate_on.")
        @JsonProperty("delete_from_element_when")
        private String deleteFromElementWhen;

        @JsonPropertyDescription(
                "Specifies the key of the list of object to iterate over and delete the keys specified in with_keys.")
        @JsonProperty("iterate_on")
        private String iterateOn;

        @AssertTrue(message = "exclude_from_delete only applies when with_keys_regex is configured.")
        boolean isExcludeFromDeleteValid() {
            return !((excludeFromDelete != null && !excludeFromDelete.isEmpty()) && (withKeysRegex == null || withKeysRegex.isEmpty()));
        }

        @JsonIgnore
        private List<Pattern> withKeysRegexPatterns;

        private void setWithKeysRegexPatterns() {
            withKeysRegexPatterns = getWithKeysRegex().stream()
                    .map(Pattern::compile)
                    .collect(Collectors.toList());
        }

        @AssertTrue(message = "Invalid regex pattern found in with_keys_regex.")
        public boolean isValidWithKeysRegexPattern() {
            if (withKeysRegex != null && !withKeysRegex.isEmpty()) {
                try {
                    setWithKeysRegexPatterns();
                } catch (PatternSyntaxException e) {
                    return false;
                }
            }
            return true;
        }

        public List<EventKey> getWithKeys() {
            return withKeys != null ? withKeys : Collections.emptyList();
        }

        public List<String> getWithKeysRegex() {
            return withKeysRegex != null ? withKeysRegex : Collections.emptyList();
        }
        
        public List<Pattern> getWithKeysRegexPattern() {
            if (withKeysRegexPatterns == null && withKeysRegex != null && !withKeysRegex.isEmpty()) {
                setWithKeysRegexPatterns();
            }
            return withKeysRegexPatterns != null ? withKeysRegexPatterns : Collections.emptyList();
        }

        public Set<EventKey> getExcludeFromDelete() {
            return excludeFromDelete != null ? excludeFromDelete : Collections.emptySet();
        }

        public String getDeleteWhen() {
            return deleteWhen;
        }

        public String getIterateOn() { return  iterateOn; }

        public String getDeleteFromElementWhen() {
            return deleteFromElementWhen;
        }

        public Entry(final List<EventKey> withKeys,
                     final List<String> withKeysRegex,
                     final Set<EventKey> excludeFromDelete,
                     final String deleteWhen,
                     final String iterateOn,
                     final String deleteFromElementWhen) {
            this.withKeys = withKeys;
            this.withKeysRegex = withKeysRegex;
            this.excludeFromDelete = excludeFromDelete;
            this.deleteWhen = deleteWhen;
            this.deleteFromElementWhen = deleteFromElementWhen;
            this.iterateOn = iterateOn;
        }

        public Entry() {
        }
    }

    @JsonProperty("with_keys")
    @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
    @JsonPropertyDescription("A list of keys to be deleted. May not be used with entries.")
    private List<@NotNull @NotEmpty EventKey> withKeys;

    @JsonProperty("with_keys_regex")
    @JsonPropertyDescription("A list of regex patterns that match keys to be deleted from an event. May not be used with entries.")
    private List<String> withKeysRegex;

    @JsonProperty("exclude_from_delete")
    @JsonPropertyDescription("A list of keys to exclude from deletion when using with_keys_regex.")
    private Set<EventKey> excludeFromDelete;

    @JsonProperty("delete_when")
    @JsonPropertyDescription("Specifies under what condition the deletion should be performed.")
    private String deleteWhen;

    @Valid
    @JsonProperty("entries")
    @JsonPropertyDescription("A list of entries to delete from the event.")
    private List<Entry> entries;

    @AssertTrue(message = "One of the following must be provided: 'entries', 'with_keys', or 'with_keys_regex'. None of these are configured.")
    boolean isConfigurationPresent() {
        return entries != null || withKeys != null || withKeysRegex != null;
    }

    @AssertTrue(message = "You can only use one of the following at a time: 'entries', 'with_keys', or 'with_keys_regex'")
    boolean hasOnlyOneConfiguration() {
        int count = 0;
        if (entries != null) count++;
        if (withKeys != null) count++;
        if (withKeysRegex != null) count++;
        return count == 1;
    }

    @AssertTrue(message = "exclude_from_delete only applies when with_keys_regex is configured.")
    boolean isExcludeFromDeleteValid() {
        return !((excludeFromDelete != null && !excludeFromDelete.isEmpty()) && (withKeysRegex == null || withKeysRegex.isEmpty()));
    }

    @JsonIgnore
    private List<Pattern> withKeysRegexPatterns;

    private void setWithKeysRegexPatterns() {
        withKeysRegexPatterns = getWithKeysRegex().stream()
                .map(Pattern::compile)
                .collect(Collectors.toList());
    }

    @AssertTrue(message = "Invalid regex pattern in 'with_keys_regex'")
    boolean isValidWithKeysRegexPattern() {
        if (withKeysRegex != null && !withKeysRegex.isEmpty()) {
            try {
                setWithKeysRegexPatterns();
            } catch (PatternSyntaxException e) {
                return false;
            }
        }
        return true;
    }

    @JsonPropertyDescription(
            "Specifies the key of the list of object to iterate over and delete the keys specified in with_keys.")
    @JsonProperty("iterate_on")
    private String iterateOn;

    @JsonPropertyDescription("Specifies the condition for when to delete the key from each element of a list when using iterate_on.")
    @JsonProperty("delete_from_element_when")
    private String deleteFromElementWhen;

    public List<EventKey> getWithKeys() {
        return withKeys != null ? withKeys : Collections.emptyList();
    }

    public List<String> getWithKeysRegex() {
        return withKeysRegex != null ? withKeysRegex : Collections.emptyList();
    }

    public List<Pattern> getWithKeysRegexPattern() {
        if (withKeysRegexPatterns == null && withKeysRegex != null && !withKeysRegex.isEmpty()) {
            setWithKeysRegexPatterns();
        }
        return withKeysRegexPatterns != null ? withKeysRegexPatterns : Collections.emptyList();
    }

    public Set<EventKey> getExcludeFromDelete() {
        return excludeFromDelete != null ? excludeFromDelete : Collections.emptySet();
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public String getIterateOn() {
        return iterateOn;
    }

    public String getDeleteFromElementWhen() {
        return deleteFromElementWhen;
    }
}
