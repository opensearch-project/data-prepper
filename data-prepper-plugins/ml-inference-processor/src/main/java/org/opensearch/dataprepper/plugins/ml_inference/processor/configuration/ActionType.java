/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public enum ActionType {
    PREDICT("predict", "_predict"),
    BATCH_PREDICT("batch_predict", "_batch_predict");

    private final String userInputValue;
    private final String mlCommonsValue;

    ActionType(String userInputValue, String mlCommonsValue) {
        this.userInputValue = userInputValue;
        this.mlCommonsValue = mlCommonsValue;
    }

    @JsonValue
    public String getUserInputValue() {
        return userInputValue;
    }

    public String getMlCommonsActionValue(){
        return mlCommonsValue;
    }

    private static final Map<String, ActionType> INVOCATION_TYPE_MAP = new HashMap<>();

    static {
        for (ActionType type : ActionType.values()) {
            INVOCATION_TYPE_MAP.put(type.getUserInputValue(), type);
        }
    }

    @JsonCreator
    public static ActionType fromString(String value) {
        return INVOCATION_TYPE_MAP.get(value);
    }
}
