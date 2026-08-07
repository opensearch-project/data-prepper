/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
