/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An enum class which helps to identify the different authentication types for
 * user login.
 */

public enum AuthenticationType {

    PLAINTEXT("plaintext"), SSL("ssl"), OAUTH("oauth");
    private static final Map<String, AuthenticationType> AUTHENTICATION_TYPE_MAP = Arrays.stream(AuthenticationType.values())
            .collect(Collectors.toMap(AuthenticationType::toString, Function.identity()));

    private final String authType;

    AuthenticationType(String authType) {
        this.authType = authType;
    }

    @Override
    public String toString() {
        return this.authType;
    }

    public static AuthenticationType getAuthTypeByName(final String name) {
        return AUTHENTICATION_TYPE_MAP.get(name.toLowerCase());
    }
}
