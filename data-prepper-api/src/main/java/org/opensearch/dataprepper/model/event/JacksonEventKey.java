/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.core.JsonPointer;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

class JacksonEventKey implements EventKey {
    private static final String SEPARATOR = "/";
    private static final int MAX_KEY_LENGTH = 2048;
    private final String key;
    private final EventKeyFactory.EventAction[] eventActions;
    private final String trimmedKey;
    private final List<String> keyPathList;
    private final JsonPointer jsonPointer;
    private final Set<EventKeyFactory.EventAction> supportedActions;

    JacksonEventKey(final String key, final EventKeyFactory.EventAction... eventActions) {
        this.key = Objects.requireNonNull(key, "Parameter key cannot be null for EventKey.");
        this.eventActions = eventActions.length == 0 ? new EventKeyFactory.EventAction[] { EventKeyFactory.EventAction.ALL } : eventActions;

        if(key.isEmpty()) {
            for (final EventKeyFactory.EventAction action : this.eventActions) {
                if (action.isMutableAction()) {
                    throw new IllegalArgumentException("Event key cannot be an empty string for " + action + " actions.");
                }
            }
        }

        trimmedKey = checkAndTrimKey(key);

        keyPathList = Collections.unmodifiableList(Arrays.asList(trimmedKey.split(SEPARATOR, -1)));
        jsonPointer = toJsonPointer(trimmedKey);

        supportedActions = EnumSet.noneOf(EventKeyFactory.EventAction.class);
        for (final EventKeyFactory.EventAction eventAction : this.eventActions) {
            supportedActions.addAll(eventAction.getSupportedActions());
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
        return keyPathList;
    }

    JsonPointer getJsonPointer() {
        return jsonPointer;
    }

    boolean supports(final EventKeyFactory.EventAction eventAction) {
        return supportedActions.contains(eventAction);
    }

    private String checkAndTrimKey(final String key) {
        checkKey(key);
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

    private static boolean isValidKey(final String key) {
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);

            if (!(c >= 48 && c <= 57
                    || c >= 65 && c <= 90
                    || c >= 97 && c <= 122
                    || c == '.'
                    || c == '-'
                    || c == '_'
                    || c == '@'
                    || c == '/'
                    || c == '['
                    || c == ']')) {

                return false;
            }
        }
        return true;
    }

    private JsonPointer toJsonPointer(final String key) {
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
