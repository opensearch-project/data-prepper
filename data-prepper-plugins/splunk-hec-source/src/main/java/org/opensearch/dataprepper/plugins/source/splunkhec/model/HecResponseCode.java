/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum HecResponseCode {

    SUCCESS(0, "Success"),
    TOKEN_DISABLED(1, "Token disabled"),
    TOKEN_REQUIRED(2, "Token is required"),
    TOKEN_INVALID(3, "Invalid authorization"),
    INVALID_TOKEN(4, "Invalid token"),
    NO_DATA(5, "No data"),
    INVALID_DATA_FORMAT(6, "Invalid data format"),
    INCORRECT_INDEX(7, "Incorrect index"),
    INTERNAL_SERVER_ERROR(8, "Internal server error"),
    SERVER_BUSY(9, "Server is busy"),
    DATA_CHANNEL_MISSING(10, "Data channel is missing"),
    INVALID_DATA_CHANNEL(11, "Invalid data channel"),
    EVENT_FIELD_REQUIRED(12, "Event field is required"),
    EVENT_FIELD_BLANK(13, "Event field cannot be blank"),
    ACK_DISABLED(14, "ACK is disabled"),
    ERROR_HANDLING_ACK(15, "Error in handling indexed fields"),
    QUERY_STRING_AUTH_NOT_ENABLED(16, "Query string authorization is not enabled"),
    HEC_HEALTHY(17, "HEC is healthy"),
    HEC_UNHEALTHY(18, "HEC is unhealthy");

    private static final Map<Integer, HecResponseCode> CODE_MAP = Arrays.stream(HecResponseCode.values())
            .collect(Collectors.toMap(HecResponseCode::getCode, Function.identity()));

    private final int code;
    private final String text;

    HecResponseCode(final int code, final String text) {
        this.code = code;
        this.text = text;
    }

    @JsonValue
    public int getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

    @JsonCreator
    public static HecResponseCode fromCode(final int code) {
        final HecResponseCode responseCode = CODE_MAP.get(code);
        if (responseCode == null) {
            throw new IllegalArgumentException("Unknown HEC response code: " + code);
        }
        return responseCode;
    }
}
