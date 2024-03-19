/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import java.util.Arrays;

enum IpAddressGenerationOption {
    OFF("off"),
    DEFINED("defined"),
    GENERATED("generated");

    private final String value;

    String getValue() {
        return value;
    }

    IpAddressGenerationOption(final String value) {
        this.value = value;
    }

    static IpAddressGenerationOption fromValue(final String value) {
        return Arrays.stream(IpAddressGenerationOption.values())
                .filter(option -> option.value.equals(value))
                .findFirst()
                .orElse(null);
    }
}
