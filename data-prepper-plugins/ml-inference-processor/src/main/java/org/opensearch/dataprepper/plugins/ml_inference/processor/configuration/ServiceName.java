/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public enum ServiceName {
    SAGEMAKER("sagemaker", "sagemaker"),
    BEDROCK("bedrock", "bedrock");

    private final String userInputValue;
    private final String mlCommonsValue;

    ServiceName(String userInputValue, String mlCommonsValue) {
        this.userInputValue = userInputValue;
        this.mlCommonsValue = mlCommonsValue;
    }

    @JsonValue
    public String getUserInputValue() {
        return userInputValue;
    }

    public String getMlCommonsValue(){
        return mlCommonsValue;
    }

    private static final Map<String, ServiceName> SERVICE_NAME_MAP = new HashMap<>();

    static {
        for (ServiceName type : ServiceName.values()) {
            SERVICE_NAME_MAP.put(type.getUserInputValue(), type);
        }
    }

    @JsonCreator
    public static ServiceName fromString(String value) {
        return SERVICE_NAME_MAP.get(value);
    }

}
