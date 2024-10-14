/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum ForwardingAuthentication {
    MUTUAL_TLS("mutual_tls"),
    UNAUTHENTICATED("unauthenticated");

    private static final Map<String, ForwardingAuthentication> STRING_NAME_TO_ENUM_MAP = new HashMap<>();

    private final String name;

    static {
        Arrays.stream(ForwardingAuthentication.values())
                .forEach(enumValue -> STRING_NAME_TO_ENUM_MAP.put(enumValue.name, enumValue));
    }

    ForwardingAuthentication(final String name){
        this.name = name;
    }

    public String getName(){
        return name;
    }

    static ForwardingAuthentication getByName(final String name) {
        return Optional.ofNullable(STRING_NAME_TO_ENUM_MAP.get(name))
                .orElseThrow(() -> new IllegalArgumentException("Unrecognized ForwardingAuthentication: " + name));
    }
}
