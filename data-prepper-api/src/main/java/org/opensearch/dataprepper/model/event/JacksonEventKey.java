/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.core.JsonPointer;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

class JacksonEventKey implements EventKey {
    private static final String INVALID_KEY_REPLACEMENT = "_";
    private static final String SEPARATOR = "/";
    private static final int MAX_KEY_LENGTH = 2048;
    private final String key;
    private final EventKeyFactory.EventAction[] eventActions;
    private final String trimmedKey;
    private List<String> keyPathList;
    private JsonPointer jsonPointer;
    private final Set<EventKeyFactory.EventAction> supportedActions;
    private static final Pattern INVALID_KEY_CHARS_PATTERN =
            Pattern.compile("[^A-Za-z0-9._~@/\\[\\]-]");

    /**
     * Constructor for the JacksonEventKey which should only be used by implementation
     * of {@link EventKeyFactory} in Data Prepper core.
     *
     * @param key The key
     * @param eventActions Event actions to support
     */
    JacksonEventKey(final String key, final EventKeyFactory.EventAction... eventActions) {
        this(key, false, eventActions);
    }

    /**
     * Constructs a new JacksonEventKey.
     * <p>
     * This overload should only be used by {@link JacksonEvent} directly. It allows for skipping creating
     * some resources knowing they will not be needed. The {@link JacksonEvent} only needs a JSON pointer
     * when performing GET event actions. So we can optimize PUT/DELETE actions when called with a string
     * key instead of an EventKey by not creating the JSON Pointer at all.
     * <p>
     * For EventKey's constructed through the factory, we should not perform lazy initialization since
     * we may lose some possible validations.
     *
     * @param key the key
     * @param lazy Use true to lazily initialize. This will not be thread-safe, however.
     * @param eventActions Event actions to support
     */
    JacksonEventKey(final String key, final boolean lazy, final EventKeyFactory.EventAction... eventActions) {
        this.key = Objects.requireNonNull(key, "Parameter key cannot be null for EventKey.");
        this.eventActions = eventActions.length == 0 ? new EventKeyFactory.EventAction[] { EventKeyFactory.EventAction.ALL } : eventActions;

        supportedActions = EnumSet.noneOf(EventKeyFactory.EventAction.class);
        for (final EventKeyFactory.EventAction eventAction : this.eventActions) {
            supportedActions.addAll(eventAction.getSupportedActions());
        }

        if(key.isEmpty()) {
            for (final EventKeyFactory.EventAction action : this.eventActions) {
                if (action.isMutableAction()) {
                    throw new IllegalArgumentException("Event key cannot be an empty string for " + action + " actions.");
                }
            }
        }

        trimmedKey = checkAndTrimKey(key);

        if(!lazy) {
            keyPathList = toKeyPathList();
            jsonPointer = toJsonPointer(trimmedKey);
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    String getTrimmedKey() {
        return trimmedKey;
    }

    List<String> getKeyPathList() {
        if(keyPathList == null) {
            keyPathList = toKeyPathList();
        }
        return keyPathList;
    }

    JsonPointer getJsonPointer() {
        if(jsonPointer == null) {
            jsonPointer = toJsonPointer(trimmedKey);
        }
        return jsonPointer;
    }

    boolean supports(final EventKeyFactory.EventAction eventAction) {
        return supportedActions.contains(eventAction);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other)
            return true;
        if (other == null || getClass() != other.getClass())
            return false;
        final JacksonEventKey that = (JacksonEventKey) other;
        return Objects.equals(key, that.key) && Arrays.equals(eventActions, that.eventActions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(eventActions));
    }

    @Override
    public String toString() {
        return key;
    }

    private String checkAndTrimKey(final String key) {
        if(!supportedActions.equals(Collections.singleton(EventKeyFactory.EventAction.DELETE)))
        {
            checkKey(key);
        }
        return trimTrailingSlashInKey(key);
    }

    private static void checkKey(final String key) {
        checkNotNull(key, "key cannot be null");
        if (key.isEmpty()) {
            // Empty string key is valid
            return;
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("key cannot be longer than " + MAX_KEY_LENGTH + " characters");
        }
        if (!isValidKey(key)) {
            throw new IllegalArgumentException("key " + key + " must contain only alphanumeric chars with .-_@/ and must follow JsonPointer (ie. 'field/to/key')");
        }
    }


    static String trimTrailingSlashInKey(final String key) {
        return key.length() > 1 && key.endsWith(SEPARATOR) ? key.substring(0, key.length() - 1) : key;
    }

    static String replaceInvalidCharacters(final String key) {
        if (key == null) {
            return null;
        }
        if (isValidKey(key)) {
            return key;
        }
        return INVALID_KEY_CHARS_PATTERN.matcher(key).replaceAll(INVALID_KEY_REPLACEMENT);
    }

    private static boolean isValidKey(final String key) {
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);

            if (!(c >= 48 && c <= 57
                    || c >= 65 && c <= 90
                    || c >= 97 && c <= 122
                    || c == '.'
                    || c == '-'
                    || c == '_'
                    || c == '~'
                    || c == '@'
                    || c == '/'
                    || c == '['
                    || c == ']'
                    || c == ' '
                    || c == '$'
                    || c == '('
                    || c == ')'
                    || c == '%'
                    || c == ':'
            )) {

                return false;
            }
        }
        return true;
    }

    private List<String> toKeyPathList() {
        return Collections.unmodifiableList(Arrays.asList(trimmedKey.split(SEPARATOR, -1)));
    }

    private static JsonPointer toJsonPointer(final String key) {
        final String jsonPointerExpression;
        if (key.isEmpty() || key.startsWith("/")) {
            jsonPointerExpression = key;
        } else {
            jsonPointerExpression = SEPARATOR + key;
        }
        return JsonPointer.compile(jsonPointerExpression);
    }

    static boolean isValidEventKey(final String key) {
        try {
            checkKey(key);
            return true;
        } catch (final Exception e) {
            return false;
        }
    }
}
