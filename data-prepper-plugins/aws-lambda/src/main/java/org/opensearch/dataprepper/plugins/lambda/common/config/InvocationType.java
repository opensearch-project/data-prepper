package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public enum InvocationType {
    REQUEST_RESPONSE("request-response", "RequestResponse"),
    EVENT("event", "Event");

    private final String userInputValue;
    private final String awsLambdaValue;

    InvocationType(String userInputValue, String awsLambdaValue) {
        this.userInputValue = userInputValue;
        this.awsLambdaValue = awsLambdaValue;
    }

    @JsonValue
    public String getUserInputValue() {
        return userInputValue;
    }

    public String getAwsLambdaValue(){
        return awsLambdaValue;
    }

    private static final Map<String, InvocationType> INVOCATION_TYPE_MAP = new HashMap<>();

    static {
        for (InvocationType type : InvocationType.values()) {
            INVOCATION_TYPE_MAP.put(type.getUserInputValue(), type);
        }
    }

    @JsonCreator
    public static InvocationType fromString(String value) {
        return INVOCATION_TYPE_MAP.get(value);
    }
}

